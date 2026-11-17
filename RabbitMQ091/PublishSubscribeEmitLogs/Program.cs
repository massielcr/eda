using RabbitMQ.Client;
using System.Text;

public class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("Emmiting Logs...");

        string message = (args.Length > 0) ? string.Join(" ", args) : "info: Hello World!";

        await EmitLog(
            message: message,
            exchangeName: "logs",
            hostName: "localhost");

        Console.WriteLine(" Press [enter] to exit.");
        Console.ReadLine();
    }

    public static async Task EmitLog(string message, string exchangeName, string hostName)
    {
        var factory = new ConnectionFactory() { HostName = hostName };
        using IConnection connection = await factory.CreateConnectionAsync();
        using IChannel channel = await connection.CreateChannelAsync();

        await channel.ExchangeDeclareAsync(exchange: exchangeName, type: ExchangeType.Fanout);

        byte[] body = Encoding.UTF8.GetBytes(message);

        await channel.BasicPublishAsync(exchange: exchangeName, routingKey: string.Empty, body: body);

        Console.WriteLine($" [x] Sent {message}");
    }
}