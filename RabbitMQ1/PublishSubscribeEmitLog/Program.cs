using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using System.Text;

public class Program
{
    const string _brokerUri = "amqp://guest:guest@localhost:5672/%2f";
    const string _exchangeName = "logs";

    public static async Task Main(string[] args)
    {
        Console.WriteLine("Emitting Logs...");

        string message = args.Length > 0 ? string.Join(" ", args) : "info: Hello World Log!";

        await EmitLog(message);

        Console.ReadLine();
    }

    private static async Task EmitLog(string message)
    {
        ConnectionSettings settings = ConnectionSettingsBuilder.Create()
            .Uri(new Uri(_brokerUri))
            .ContainerId("tutorial-emitlog")
            .Build();

        IEnvironment environment = AmqpEnvironment.Create(settings);

        IConnection connection = await environment.CreateConnectionAsync();

        try
        {
            IManagement management = connection.Management();

            IExchangeSpecification exchangeSpecification = management.Exchange(_exchangeName).Type("fanout");
            await exchangeSpecification.DeclareAsync();

            IPublisher publisher = await connection.PublisherBuilder().Exchange(_exchangeName).BuildAsync();

            try
            {
                AmqpMessage amqpMessage = new(Encoding.UTF8.GetBytes(message));

                PublishResult publishResult = await publisher.PublishAsync(amqpMessage);

                switch (publishResult.Outcome.State)
                {
                    case OutcomeState.Accepted:
                        Console.WriteLine($" [x] Sent '{message}'");
                        break;
                    case OutcomeState.Released:
                        Console.Error.WriteLine($"Released message: {publishResult.Message.BodyAsString()}");
                        break;
                    case OutcomeState.Rejected:
                        Console.Error.WriteLine($"[Publisher] Message: {publishResult.Message.BodyAsString()} rejected with error: {publishResult.Outcome.Error}");
                        break;
                    default:
                        Console.Error.WriteLine($"Unexpected publish outcome: {publishResult.Outcome.State}");
                        break;                }
            }
            finally
            {
                await publisher.CloseAsync();
            }
        }
        finally
        {
            await connection.CloseAsync();
            await environment.CloseAsync();
        }
    }
}


