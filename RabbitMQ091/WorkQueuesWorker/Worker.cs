using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;


public static class Worker
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("Consuming...");

        await ConsumeAsync(
            queueName: "task_queue",
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

        await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);

        Console.WriteLine(" [*] Waiting for messages..");

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (model, ea) =>
        {
            var body = ea.Body.ToArray();
            string message = Encoding.UTF8.GetString(body);
            Console.WriteLine($" [x] Received {message}");

            int dots = message.Split('.').Length - 1;
            await Task.Delay(dots * 1000);

            Console.WriteLine(" [x] Done");

            await channel.BasicAckAsync(
                deliveryTag: ea.DeliveryTag,
                multiple: false);
        };

        await channel.BasicConsumeAsync(queueName, autoAck: false, consumer: consumer);

        var tcs = new TaskCompletionSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            tcs.TrySetResult();
        };

        await tcs.Task;
    }
}





