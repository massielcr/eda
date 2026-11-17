using RabbitMQ.Client;
using System.Text;

public class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("Emitting  Logs..");

        string routingKey = args.Length > 0 ? args[0].ToLower() : "anonymous.info";
        string message = args.Length > 1 ? string.Join(" ", args.Skip(1).ToArray()) : "Hello World!";

        await EmitLog(
            message: message,
            routingKey: routingKey,
            exchangename: "topic_logs",
            hostName: "localhost"
            );


        Console.WriteLine(" Press [enter] to exit.");
        Console.ReadLine();
    }

    private static async Task EmitLog(string message, string routingKey, string exchangename, string hostName)
    {
        var factory = new ConnectionFactory() { HostName = hostName };
        using IConnection connection = await factory.CreateConnectionAsync();
        using IChannel channel = await connection.CreateChannelAsync();

        await channel.ExchangeDeclareAsync(exchange: exchangename, type: ExchangeType.Topic);

        byte[] body = Encoding.UTF8.GetBytes(message);

        await channel.BasicPublishAsync(exchangename, routingKey, body);

        Console.WriteLine($" [x] Sent '{routingKey}':'{message}'");
    }
}