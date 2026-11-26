using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using System.Text;

public class Program
{
    const string _brokerUri = "amqp://guest:guest@localhost:5672/%2f";
    const string _exchangeName = "logs_direct";

    public static async Task Main(string[] args)
    {
        Console.WriteLine("Emitting Logs...");

        string severity = args.Length > 0 ? args[0] : "info";
        string message = args.Length > 1 ? string.Join(" ", args.Skip(1)) : "Hello World Direct Log!";

        await EmitLog(severity, message);

        Console.ReadLine();
    }

    private static async Task EmitLog(string severity, string message)
    {
        ConnectionSettings settings = ConnectionSettingsBuilder.Create()
            .Uri(new Uri(_brokerUri))
            .ContainerId("tutorial-emitlogdirect")
            .Build();

        IEnvironment environment = AmqpEnvironment.Create(settings);

        IConnection connection = await environment.CreateConnectionAsync();        

        try
        {
            IManagement management = connection.Management();

            IExchangeSpecification exchangeSpecification = management.Exchange(_exchangeName).Type("direct");
            await exchangeSpecification.DeclareAsync();

            IPublisher publisher = await connection.PublisherBuilder().Exchange(_exchangeName).Key(severity).BuildAsync();

            try
            {
                AmqpMessage amqpMessage = new AmqpMessage(Encoding.UTF8.GetBytes(message));

                PublishResult publishResult = await publisher.PublishAsync(amqpMessage);

                switch (publishResult.Outcome.State)
                {
                    case OutcomeState.Accepted:
                        Console.WriteLine($" [x] Sent '{severity}':'{message}'");
                        break;
                    case OutcomeState.Released:
                        Console.Error.WriteLine($"Released message: {publishResult.Message.BodyAsString()}");
                        break;
                    case OutcomeState.Rejected:
                        Console.Error.WriteLine($"[Publisher] Message: {publishResult.Message.BodyAsString()} rejected with error: {publishResult.Outcome.Error}");
                        break;
                    default:
                        Console.Error.WriteLine($"Unexpected publish outcome: {publishResult.Outcome.State}");
                        break;
                }
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
