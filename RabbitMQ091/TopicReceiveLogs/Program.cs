using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

public class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("Receiving Logs...");

        string routingKey = args.Length > 0 ? args[0].ToLower() : "invalid";

        await ReceiveLog(
            exchangeName: "topic_logs",
            routingKey: routingKey,
            hostName: "localhost");

        Console.WriteLine(" Press [enter] to exit.");
        Console.ReadLine();
    }

    private static async Task ReceiveLog(string exchangeName, string routingKey, string hostName)
    {
        var factory = new ConnectionFactory() { HostName = hostName };
        using IConnection connection = await factory.CreateConnectionAsync();
        using IChannel channel = await connection.CreateChannelAsync();

        await channel.ExchangeDeclareAsync(exchange: exchangeName, type: ExchangeType.Topic);

        QueueDeclareOk queue = await channel.QueueDeclareAsync();
        string queueName = queue.QueueName;

        await channel.QueueBindAsync(queueName, exchangeName, routingKey);

        Console.WriteLine(" [*] Waiting for messages. To exit press CTRL+C");

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += (model, ea) =>
        {
            byte[] body = ea.Body.ToArray();
            string message = Encoding.UTF8.GetString(body);

            string routingKey = ea.RoutingKey;

            Console.WriteLine($" [x] Received '{routingKey}':'{message}'");

            return Task.CompletedTask;
        };

        await channel.BasicConsumeAsync(queueName, autoAck: true, consumer);

        var tcs = new TaskCompletionSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            tcs.TrySetResult();
        };

        await tcs.Task;
    }
}
