using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Collections.Concurrent;
using System.Text;

namespace RPCClient
{
    public class RpcClient : IAsyncDisposable
    {
        private readonly string _queueName;
        private string? _replyQueueName;
        private readonly IConnectionFactory _connectionFactory;
        private IConnection? _connection;
        private IChannel? _channel;

        private readonly ConcurrentDictionary<string, TaskCompletionSource<int>> _callbackMapper = new();

        public RpcClient(string queueName, string hostName)
        {
            _queueName = queueName;
            _connectionFactory = new ConnectionFactory() { HostName = hostName };
        }

        public async Task StartAsync()
        {
            _connection = await _connectionFactory.CreateConnectionAsync();
            _channel = await _connection.CreateChannelAsync();

            QueueDeclareOk queue = await _channel.QueueDeclareAsync();
            _replyQueueName = queue.QueueName;

            var consumer = new AsyncEventingBasicConsumer(_channel);
            consumer.ReceivedAsync += (model, ea) =>
            {
                string? correlationId = ea.BasicProperties.CorrelationId;
                if (!string.IsNullOrWhiteSpace(correlationId))
                {
                    if (_callbackMapper.TryRemove(correlationId, out var tcs))
                    {
                        byte[] body = ea.Body.ToArray();
                        string response = Encoding.UTF8.GetString(body);

                        if (int.TryParse(response, out int n))
                        {
                            tcs.TrySetResult(n);
                        }
                        else
                        {
                            tcs.TrySetException(new FormatException($"Unexpected response: '{response}'"));
                        }
                    }
                }

                return Task.CompletedTask;
            };

            await _channel.BasicConsumeAsync(_replyQueueName, autoAck: true, consumer);
        }

        public async Task<int> CallAsync(int n, CancellationToken token = default)
        {
            if (_channel is null) { throw new InvalidOperationException("Client has not been started. Call StartAsync() first."); }

            string correlationId = Guid.NewGuid().ToString();
            BasicProperties properties = new()
            {
                CorrelationId = correlationId,
                ReplyTo = _replyQueueName
            };

            TaskCompletionSource<int> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
            _callbackMapper.TryAdd(correlationId, tcs);

            try
            {
                byte[] body = Encoding.UTF8.GetBytes(n.ToString());

                await _channel.BasicPublishAsync(
                    exchange: string.Empty,
                    routingKey: _queueName,
                    mandatory: true,
                    basicProperties: properties,
                    body: body,
                    cancellationToken: token);

                using CancellationTokenRegistration ctr = token.Register(() =>
                {
                    _callbackMapper.TryRemove(correlationId, out _);
                    tcs.TrySetCanceled(token);
                });

                return await tcs.Task;
            }
            finally
            {
                _callbackMapper.TryRemove(correlationId, out _);
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
