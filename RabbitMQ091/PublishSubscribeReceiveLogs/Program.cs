using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;


public class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("Receiving Logs..");

        await ReceiveLog(
            exchangeName: "logs",
            hostName: "localhost");

        Console.WriteLine(" Press [enter] to exit.");
        Console.ReadLine();
    }

    public static async Task ReceiveLog(string exchangeName, string hostName)
    {
        var factory = new ConnectionFactory() { HostName = hostName };
        using IConnection connection = await factory.CreateConnectionAsync();
        using IChannel channel = await connection.CreateChannelAsync();

        await channel.ExchangeDeclareAsync(exchange: exchangeName, type: ExchangeType.Fanout);

        QueueDeclareOk queue = await channel.QueueDeclareAsync();
        string queueName = queue.QueueName;
        await channel.QueueBindAsync(queue: queueName, exchange: exchangeName, routingKey: string.Empty);

        Console.WriteLine(" [*] Waiting for logs.");

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += (model, ea) =>
        {
            byte[] body = ea.Body.ToArray();
            var message = Encoding.UTF8.GetString(body);

            Console.WriteLine($" [x] {message}");

            return Task.CompletedTask;
        };

        await channel.BasicConsumeAsync(queueName, autoAck: true, consumer: consumer);

        var tcs = new TaskCompletionSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            tcs.TrySetResult();
        };

        await tcs.Task;
    }
}
