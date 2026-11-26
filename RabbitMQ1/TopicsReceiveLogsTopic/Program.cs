using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;

public class Program
{
    const string _brokerUri = "amqp://guest:guest@localhost:5672/%2f";
    const string _exchangeName = "logs_topic";

    public static async Task Main(string[] args)
    {
        Console.WriteLine("Receiving Logs...");

        string routing = args.Length > 0 ? args[0] : "anonymous.info";

        await ConsumeLog(routing);

        Console.ReadLine();
    }

    private static async Task ConsumeLog(string routingKey)
    {
        ConnectionSettings settings = ConnectionSettingsBuilder.Create()
            .Uri(new Uri(_brokerUri))
            .ContainerId("")
            .Build();

        IEnvironment environment = AmqpEnvironment.Create(settings);

        IConnection connection = await environment.CreateConnectionAsync();

        try
        {
            IManagement management = connection.Management();

            IExchangeSpecification exchangeSpecification = management.Exchange(_exchangeName).Type("topic");
            await exchangeSpecification.DeclareAsync();

            IQueueSpecification queueSpecification = management.Queue().Exclusive(true).AutoDelete(true);
            IQueueInfo queueInfo = await queueSpecification.DeclareAsync();
            string queueName = queueInfo.Name();

            IBindingSpecification bindingSpecification = management.Binding()
                .SourceExchange(_exchangeName)
                .DestinationQueue(queueName)
                .Key(routingKey);
            await bindingSpecification.BindAsync();

            IConsumer consumer = await connection.ConsumerBuilder()
                .Queue(queueName)
                .MessageHandler((ctx, message) =>
                {
                    string body = message.BodyAsString();

                    string routingKey = "";
                    object? rk = message.Annotation("x-routing-key");
                    if (rk is not null)
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