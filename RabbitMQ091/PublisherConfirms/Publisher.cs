using RabbitMQ.Client;
using System.Text;

namespace PublisherConfirms
{
    public class Publisher : IAsyncDisposable
    {
        const ushort MAX_OUTSTANDING_CONFIRMS = 256;

        private readonly IConnectionFactory _connectionFactory;

        private IConnection? _connection;
        private IChannel? _channel;

        private string? _queueName;

        public Publisher(string hostName)
        {
            _connectionFactory = new ConnectionFactory() { HostName = hostName };
        }

        public async Task InitializedAsync()
        {
            if (_connection is not null && _channel is not null && _queueName is not null)
                return;

            _connection = await _connectionFactory.CreateConnectionAsync();

            CreateChannelOptions channelOptions = new(
                publisherConfirmationsEnabled: true,
                publisherConfirmationTrackingEnabled: true,
                outstandingPublisherConfirmationsRateLimiter: new ThrottlingRateLimiter(MAX_OUTSTANDING_CONFIRMS));

            _channel = await _connection.CreateChannelAsync(channelOptions);

            QueueDeclareOk queue = await _channel.QueueDeclareAsync();
            _queueName = queue.QueueName;
        }


        public async Task SendIndviduallyAsync(string message)
        {
            if (_channel is null || _queueName is null)
                throw new InvalidOperationException("Call InitializedAsync() before publishing.");


            BasicProperties properties = new()
            {
                Persistent = true
            };      

            Console.WriteLine($"{DateTime.Now} [INFO] publishing '{message}' message and handling confirms per-message");

            try
            {
                byte[] body = Encoding.UTF8.GetBytes(message);

                await _channel.BasicPublishAsync(exchange: string.Empty, routingKey: _queueName, body: body, basicProperties: properties, mandatory: true);
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"{DateTime.Now} [ERROR] saw nack or return, ex: {ex}");
            }

            Console.WriteLine($"{DateTime.Now} [INFO] published message individually");
        }

        public async Task SendInBatchAsync(string[] messages)
        {

        }

        public async Task HandlePublishConfirmsAsync(string[] messages)
        {

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
