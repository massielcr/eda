using RabbitMQ.Client;
using System.Text;

public class Send
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("Producing...");

        string message = (args.Length > 0) ? string.Join(" ", args) : "Hello Work Queue 0.9.1!";

        await SendMessageAsync(
            message: message,
            queueName: "workqueue",
            hostName: "localhost");
    }

    private static async Task SendMessageAsync(string message, string queueName, string hostName)
    {
        var factory = new ConnectionFactory() { HostName = hostName };

        await using IConnection connection = await factory.CreateConnectionAsync();
        await using IChannel channel = await connection.CreateChannelAsync();

        await channel.QueueDeclareAsync(
            queue: queueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: new Dictionary<string, object?> { { "x-queue-type", "quorum" } });

        byte[] body = Encoding.UTF8.GetBytes(message);

        await channel.BasicPublishAsync(
            exchange: string.Empty, 
            routingKey: queueName, 
            body: body);

        Console.WriteLine($" [x] Sent {message}");
    }
}

