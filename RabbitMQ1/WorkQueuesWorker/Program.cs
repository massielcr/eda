using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;

public class Program
{
    const string _brokerUri = "amqp://guest:guest@localhost:5672/%2f";
    const string _queueName = "task_queue";

    public static async Task Main(string[] args)
    {
        Console.WriteLine("Consuming...");

        await Consume();

        Console.ReadLine();
    }

    private static async Task Consume()
    {
        ConnectionSettings settings = ConnectionSettingsBuilder.Create()
            .Uri(new Uri(_brokerUri))
            .ContainerId("tutorial-worker")
            .Build();

        IEnvironment environment = AmqpEnvironment.Create(settings);

        IConnection connection = await environment.CreateConnectionAsync();

        try
        {
            IManagement management = connection.Management();

            IQueueSpecification queueSpecification = management.Queue(_queueName).Type(QueueType.QUORUM);
            await queueSpecification.DeclareAsync();

            IConsumer consumer = await connection.ConsumerBuilder()
                .Queue(_queueName)
                .InitialCredits(1)
                .MessageHandler((ctx, message) =>
                {
                    string body = message.BodyAsString();

                    Console.WriteLine($" [x] Received '{body}'");

                    ctx.Accept();

                    return Task.CompletedTask;
                })
                .BuildAndStartAsync();

            try
            {
                Console.WriteLine(" [*] Waiting for messages. To exit press CTRL+C");
                using var cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true;
                    cts.Cancel();
                };
                await Task.Delay(Timeout.Infinite, cts.Token);
            }
            finally
            {
                await consumer.CloseAsync();
            }
        }
        finally
        {
            await connection.CloseAsync();
            await environment.CloseAsync();
        }
    }
}