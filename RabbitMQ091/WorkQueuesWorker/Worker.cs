using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;


public static class Worker
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("Consumming...");

        await ConsumeAsync(
            queueName: "workqueue",
            hostName: "localhost");
    }

    private static async Task ConsumeAsync(string queueName, string hostName)
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

        Console.WriteLine(" [*] Waiting for messages..");

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (model, ea) =>
        {
            var body = ea.Body.ToArray();
            string message = Encoding.UTF8.GetString(body);

            int dots = message.Split('.').Length - 1;

            await Task.Delay(dots * 1000);

            Console.WriteLine(" [x] Done");
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





