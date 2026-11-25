using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Buffers.Binary;
using System.Text;

namespace PublisherConfirms
{
    public class Publisher : IAsyncDisposable
    {
        const ushort MAX_OUTSTANDING_CONFIRMS = 50;

        private readonly IConnectionFactory _connectionFactory;

        private IConnection? _connection;
        private IChannel? _channel;

        public Publisher(string hostName)
        {
            _connectionFactory = new ConnectionFactory() { HostName = hostName };
        }

        public async Task InitializedAsync(string queueName, bool trackConfirmations = true)
        {
            if (_connection is not null && _channel is not null)
                return;

            _connection = await _connectionFactory.CreateConnectionAsync();

            CreateChannelOptions channelOptions = new(
                publisherConfirmationsEnabled: true,
                publisherConfirmationTrackingEnabled: trackConfirmations,
                outstandingPublisherConfirmationsRateLimiter: trackConfirmations
                                                            ? new ThrottlingRateLimiter(MAX_OUTSTANDING_CONFIRMS)
                                                            : null);

            _channel = await _connection.CreateChannelAsync(channelOptions);

            QueueDeclareOk queue = await _channel.QueueDeclareAsync(queue: queueName, durable: true, exclusive: false, autoDelete: false);
        }


        public async Task SendIndividuallyAsync(string message, string queueName)
        {
            if (_channel is null || string.IsNullOrWhiteSpace(queueName))
                throw new InvalidOperationException("Call InitializedAsync() before publishing.");           

            BasicProperties properties = new()
            {
                Persistent = true
            };      

            Console.WriteLine($"{DateTime.Now} [INFO] publishing '{message}' message and handling confirms per-message");

            try
            {
                byte[] body = Encoding.UTF8.GetBytes(message);

                await _channel.BasicPublishAsync(exchange: string.Empty, routingKey: queueName, body: body, basicProperties: properties, mandatory: true);
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"{DateTime.Now} [ERROR] saw nack or return, ex: {ex}");
            }

            Console.WriteLine($"{DateTime.Now} [INFO] published message individually");
        }


        public async Task SendInBatchAsync(string[] messages, string queueName)
        {
            if (_channel is null || string.IsNullOrWhiteSpace(queueName))
            {
                throw new InvalidOperationException("Call InitializedAsync() before publishing.");
            }           

            int batchSize = Math.Max(1, MAX_OUTSTANDING_CONFIRMS / 2);            

            List<ValueTask> tasks = new();

            for(int i = 0; i < messages.Length; i++)
            {
                byte[] body = Encoding.UTF8.GetBytes(messages[i]);
                ValueTask task = _channel.BasicPublishAsync(exchange: string.Empty, routingKey: queueName, body: body);
                tasks.Add(task);

                if (tasks.Count() >= batchSize)
                {
                    Console.WriteLine($"{DateTime.Now} [INFO] publishing '{batchSize}' messages and handling confirms in-batches");

                    await MaybeAwaitPublishes(tasks);

                    Console.WriteLine($"{DateTime.Now} [INFO] published messages in-batches");
                }
            }

            await MaybeAwaitPublishes(tasks);            
        }

        private async Task MaybeAwaitPublishes(List<ValueTask> tasks)
        {            
            foreach(ValueTask task in tasks)
            {
                try
                {
                    await task;
                }
                catch(Exception ex)
                {
                    Console.Error.WriteLine($"{DateTime.Now} [ERROR] saw nack or return, ex: '{ex}'");
                }
            }

            tasks.Clear();
        }


        public async Task HandlePublishConfirmsAsync(string[] messages, string queueName)
        {
            if (_channel is null || string.IsNullOrWhiteSpace(queueName))
            {
                throw new InvalidOperationException("Call InitializedAsync() before publishing.");
            }

            TaskCompletionSource<bool> allMessagesConfirmedTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
            LinkedList<ulong> outstandingConfirms = new();
            SemaphoreSlim semaphore = new(1, 1);
            int publishedCount = 0;


            AsyncEventHandler<BasicReturnEventArgs> returnHandler = (sender, ea) =>
            {
                ulong sequenceNumber = 0;
                if (ea.BasicProperties.Headers is not null &&
                    ea.BasicProperties.Headers.TryGetValue(Constants.PublishSequenceNumberHeader, out object? sequence) &&
                    sequence is byte[] bytes)
                {
                    sequenceNumber = BinaryPrimitives.ReadUInt64BigEndian(bytes);
                }

                Console.Error.WriteLine($"{DateTime.Now} [WARN] message {sequenceNumber} returned: {ea.ReplyText}");
                return CleanOutstandingConfirms(sequenceNumber, false, allMessagesConfirmedTcs, outstandingConfirms, () => publishedCount == messages.Length, semaphore);
            };

            AsyncEventHandler<BasicAckEventArgs> ackHandler = (sender, ea) =>
            {
                Console.Error.WriteLine($"{DateTime.Now} [SUCCESS] message {ea.DeliveryTag} acked (multiple: {ea.Multiple})");
                return CleanOutstandingConfirms(ea.DeliveryTag, ea.Multiple, allMessagesConfirmedTcs, outstandingConfirms, () => publishedCount == messages.Length, semaphore);
            };
                

            AsyncEventHandler<BasicNackEventArgs> nackHandler = (sender, ea) =>
            {
                Console.Error.WriteLine($"{DateTime.Now} [ERROR] message {ea.DeliveryTag} nacked (multiple: {ea.Multiple})");
                return CleanOutstandingConfirms(ea.DeliveryTag, ea.Multiple, allMessagesConfirmedTcs, outstandingConfirms, () => publishedCount == messages.Length, semaphore);
            };

            _channel.BasicReturnAsync += returnHandler;
            _channel.BasicAcksAsync += ackHandler;
            _channel.BasicNacksAsync += nackHandler;


            try
            {
                BasicProperties properties = new ()
                {
                    Persistent = true
                };

                for (int i = 0; i < messages.Length; i++)
                {
                    ulong nextSequenceNumber = await _channel.GetNextPublishSequenceNumberAsync();

                    byte[] body = Encoding.UTF8.GetBytes(messages[i]);

                    await semaphore.WaitAsync();

                    try
                    {
                        outstandingConfirms.AddLast(nextSequenceNumber);
                        publishedCount++;
                    }
                    finally
                    {
                        semaphore.Release();
                    }

                    await _channel.BasicPublishAsync(exchange: string.Empty, routingKey: queueName, mandatory: true, basicProperties: properties, body: body);
                }

                await allMessagesConfirmedTcs.Task.WaitAsync(TimeSpan.FromSeconds(10));
            }
            finally
            {
                _channel.BasicReturnAsync -= returnHandler;
                _channel.BasicAcksAsync -= ackHandler;
                _channel.BasicNacksAsync -= nackHandler;
            }

        }

        private async Task CleanOutstandingConfirms(ulong deliveryTag, bool multiple,
                                                    TaskCompletionSource<bool> allMessagesConfirmedTcs,
                                                    LinkedList<ulong> outstandingConfirms,
                                                    Func<bool> allPublished,
                                                    SemaphoreSlim semaphore)
        {
            await semaphore.WaitAsync();
            try
            {
                if (multiple)
                {
                    while (outstandingConfirms.First is { } node && node.Value <= deliveryTag)
                    {
                        outstandingConfirms.RemoveFirst();
                    }
                }
                else
                {
                    outstandingConfirms.Remove(deliveryTag);
                }

                if (outstandingConfirms.Count == 0 && allPublished())
                {
                    allMessagesConfirmedTcs.TrySetResult(true);
                }
            }
            finally
            {
                semaphore.Release();
            }
        }


        public async ValueTask DisposeAsync()
        {
            if (_channel is not null)
            {
                await _channel.CloseAsync();
                await _channel.DisposeAsync();
            }

            if (_connection is not null)
            {
                await _connection.CloseAsync();
                await _connection.DisposeAsync();
            }
        }
    }
}
