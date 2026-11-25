using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;

public class Program
{
    const string _brokerUri = "amqp://guest:guest@localhost:5672/%2f";
    const string _exchangeName = "logs";

    public static async Task Main(string[] args)
    {
        Console.WriteLine("Receiving Logs...");

        await Receive();

        Console.ReadLine();
    }

    private static async Task Receive()
    {
        ConnectionSettings settings = ConnectionSettingsBuilder.Create()
            .Uri(new Uri(_brokerUri))
            .ContainerId("tutorial-receivelogs")
            .Build();

        IEnvironment environment = AmqpEnvironment.Create(settings);

        IConnection connection = await environment.CreateConnectionAsync();

        try
        {
            IManagement management = connection.Management();

            IExchangeSpecification exchangeSpecification = management.Exchange(_exchangeName).Type("fanout");
            await exchangeSpecification.DeclareAsync();

            IQueueSpecification queueSpecification = management.Queue().Exclusive(true).AutoDelete(true);
            IQueueInfo queueInfo = await queueSpecification.DeclareAsync();
            string queueName = queueInfo.Name();

            IBindingSpecification bindingSpecification = management.Binding()
                .SourceExchange(_exchangeName)
                .DestinationQueue(queueName)
                .Key(string.Empty);
            await bindingSpecification.BindAsync();

            IConsumer consumer = await connection.ConsumerBuilder()
                .Queue(queueName)
                .MessageHandler((cts, message) =>
                {
                    string body = message.BodyAsString();

                    Console.WriteLine($" [x] Received '{body}'");

                    cts.Accept();

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
            catch(OperationCanceledException)
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


