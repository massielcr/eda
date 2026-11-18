using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

namespace RPCServer
{
    public class RpcServer : IAsyncDisposable
    {
        private readonly string _queueName;
        private readonly IConnectionFactory _connectionFactory;
        private IConnection? _connection;
        private IChannel? _channel;

        public RpcServer(string queueName, string hostName)
        {
            _queueName = queueName;
            _connectionFactory = new ConnectionFactory() { HostName = hostName };
        }

        public async Task StartAsync()
        {
            _connection = await _connectionFactory.CreateConnectionAsync();
            _channel = await _connection.CreateChannelAsync();

            await _channel.QueueDeclareAsync(
                queue: _queueName,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: new Dictionary<string, object?>() { { "x-queue-type", "quorum" } });

            await _channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);

            var consumer = new AsyncEventingBasicConsumer(_channel);
            consumer.ReceivedAsync += async (model, ea) =>
            {
                int response = 0;

                AsyncEventingBasicConsumer responseConsumer = (AsyncEventingBasicConsumer)model;
                IChannel responseChannel = responseConsumer.Channel;

                IReadOnlyBasicProperties properties = ea.BasicProperties;
                BasicProperties responseProperties = new()
                {
                    CorrelationId = properties.CorrelationId
                };

                try
                {
                    byte[] body = ea.Body.ToArray();
                    string message = Encoding.UTF8.GetString(body);
                    if (int.TryParse(message, out int input))
                    {
                        Console.WriteLine($" [.] Fib({input})");

                        response = await Task.Run(() => MathHelper.Fibonnaci(input));
                    }
                }
                catch(Exception ex)
                {
                    Console.WriteLine($" [.] {ex.Message}");
                }
                finally
                {
                    byte[] responseBody = Encoding.UTF8.GetBytes(response.ToString());


                    await responseChannel.BasicPublishAsync(
                        exchange: string.Empty,
                        routingKey: properties.ReplyTo!,
                        mandatory: true,
                        basicProperties: responseProperties,
                        body: responseBody);

                    await responseChannel.BasicAckAsync(
                        deliveryTag: ea.DeliveryTag,
                        multiple: false);
                }
            };

            await _channel.BasicConsumeAsync(_queueName, autoAck: false, consumer);
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
