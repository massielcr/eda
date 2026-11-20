using RabbitMQ.Client;
using System.Text;

namespace PublisherConfirms
{
    public class Publisher : IAsyncDisposable
    {
        const ushort MAX_OUTSTANDING_CONFIRMS = 256;
        const int MESSAGE_COUNT = 50;

        private readonly IConnectionFactory _connectionFactory;

        private IConnection? _connection;
        private IChannel? _channel;

        public Publisher(string hostName)
        {
            _connectionFactory = new ConnectionFactory() { HostName = hostName };
        }

        public async Task SendIndviduallyAsync(string message)
        {
            _connection = await _connectionFactory.CreateConnectionAsync();

            CreateChannelOptions channelOptions = new(
                publisherConfirmationsEnabled: true,
                publisherConfirmationTrackingEnabled: true,
                outstandingPublisherConfirmationsRateLimiter: new ThrottlingRateLimiter(MAX_OUTSTANDING_CONFIRMS));

            _channel = await _connection.CreateChannelAsync(channelOptions);

            BasicProperties properties = new()
            {
                Persistent = true
            };

            QueueDeclareOk queue = await _channel.QueueDeclareAsync();
            string queueName = queue.QueueName;            

            Console.WriteLine($"{DateTime.Now} [INFO] publishing {MESSAGE_COUNT:N0} messages and handling confirms per-message");

            for (int i = 0; i < MESSAGE_COUNT; i++)
            {
                try
                {
                    byte[] body = Encoding.UTF8.GetBytes($"{message} - {i}");

                    await _channel.BasicPublishAsync(exchange: string.Empty, routingKey: queueName, body: body, basicProperties: properties, mandatory: true);
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine($"{DateTime.Now} [ERROR] saw nack or return, ex: {ex}");
                }
            }

            Console.WriteLine($"{DateTime.Now} [INFO] published {MESSAGE_COUNT:N0} messages individually");
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
