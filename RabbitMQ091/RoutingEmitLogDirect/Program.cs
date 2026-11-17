using RabbitMQ.Client;
using System.Text;


public class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("Emmiting Log...");

        var severity = args.Length > 0 ? args[0] : "info";
        var message = args.Length > 1 ? string.Join(" ", args.Skip(1).ToArray()) : "Hello World!";

        await EmitLog(
            message: message,
            severity: severity,
            exchangeName: "direct_logs",
            hostName: "localhost");


        Console.WriteLine(" Press [enter] to exit.");
        Console.ReadLine();
    }

    private static async Task EmitLog(string message, string severity, string exchangeName, string hostName)
    {
        var factory = new ConnectionFactory() { HostName = hostName };
        using IConnection connection = await factory.CreateConnectionAsync();
        using IChannel channel = await connection.CreateChannelAsync();

        await channel.ExchangeDeclareAsync(exchange: exchangeName, type: ExchangeType.Direct);

        byte[] body = Encoding.UTF8.GetBytes(message);

        await channel.BasicPublishAsync(exchange: exchangeName, routingKey: severity, body: body);

        Console.WriteLine($" [x] Sent '{severity}':'{message}'");
    }
}