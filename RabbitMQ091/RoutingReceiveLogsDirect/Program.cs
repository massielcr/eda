using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

public class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("Receiving Logs...");

        HashSet<string> severities = new() { "info", "warning", "error" };

        if (args.Length < 1 || !severities.Contains(args[0].ToLower()))
        {
            Environment.ExitCode = 1;
            return;
        }

        await ReceiveLog(
            severity: args[0].ToLower(),
            exchangeName: "direct_logs",
            hostName: "localhost");

        Console.WriteLine(" Press [enter] to exit.");
        Console.ReadLine();
    }

    private static async Task ReceiveLog(string severity, string exchangeName, string hostName)
    {
        var factory = new ConnectionFactory() { HostName = hostName };
        using IConnection connection = await factory.CreateConnectionAsync();
        using IChannel channel = await connection.CreateChannelAsync();

        await channel.ExchangeDeclareAsync(exchange: exchangeName, type: ExchangeType.Direct);

        QueueDeclareOk queue = await channel.QueueDeclareAsync();
        string queueName = queue.QueueName;

        await channel.QueueBindAsync(queue: queueName, exchange: exchangeName, routingKey: severity);

        Console.WriteLine($" [*] Waiting for '{severity}' logs. Press [Ctrl+C] to exit.");

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += (model, ea) =>
        {
            byte[] body = ea.Body.ToArray();
            string message = Encoding.UTF8.GetString(body);

            string routingKey = ea.RoutingKey;

            Console.WriteLine($" [x] Received '{routingKey}':'{message}'");

            return Task.CompletedTask;
        };

        await channel.BasicConsumeAsync(queue: queueName, autoAck: true, consumer: consumer);

        var tcs = new TaskCompletionSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            tcs.TrySetResult();
        };

        await tcs.Task;
    }
}
