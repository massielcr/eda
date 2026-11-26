using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;

public class Program
{
    const string _brokerUri = "amqp://guest:guest@localhost:5672/%2f";
    const string _exchangeName = "logs_direct";

    public static async Task Main(string[] args)
    {
        Console.WriteLine("Receiving Logs..");

        await ReceiveLog(args);

        Console.ReadLine();
    }

    private static async Task ReceiveLog(string[] severities)
    {
        List<string> _validSeverities = ["info", "warning", "error"];

        ConnectionSettings settings = ConnectionSettingsBuilder.Create()
            .Uri(new Uri(_brokerUri))
            .ContainerId("tutorial-receivelogsdirect")
            .Build();

        IEnvironment environment = AmqpEnvironment.Create(settings);

        IConnection connection = await environment.CreateConnectionAsync();

        try
        {
            IManagement management = connection.Management();

            IExchangeSpecification exchangeSpecification = management.Exchange(_exchangeName).Type("direct");
            await exchangeSpecification.DeclareAsync();

            IQueueSpecification queueSpecification = management.Queue().Exclusive(true).AutoDelete(true);
            IQueueInfo queueInfo = await queueSpecification.DeclareAsync();
            string queueName = queueInfo.Name();

            foreach(string severity in severities)
            {
                if (severity is null || !_validSeverities.Contains(severity.ToLowerInvariant()))
                {
                    continue;
                }

                IBindingSpecification binding = management.Binding()
                    .SourceExchange(_exchangeName)
                    .DestinationQueue(queueName)
                    .Key(severity);
                await binding.BindAsync();
            }

            IConsumer consumer = await connection.ConsumerBuilder()
                .Queue(queueName)
                .MessageHandler((ctx, message) =>
                {
                    string body = message.BodyAsString();

                    string routingKey = "";

                    object? rk = message.Annotation("x-routing-key");
                    if (rk != null)
                    {
                        routingKey = rk.ToString() ?? "";
                    }

                    Console.WriteLine($" [x] Received '{routingKey}':'{body}'");
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
            catch (OperationCanceledException)
            {

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
