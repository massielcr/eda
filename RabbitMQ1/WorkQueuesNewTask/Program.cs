using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using System.Text;


public class Program
{
    const string _brokerUri = "amqp://guest:guest@localhost:5672/%2f";
    const string _queueName = "task_queue";

    public static async Task Main(string[] args)
    {
        Console.WriteLine("Producing...!");

        string message = args.Length > 0 ? string.Join(" ", args) : "Hello World!";

        await Produce(message);

        Console.ReadLine();
    }

    private static async Task Produce(string message)
    {
        ConnectionSettings settings = ConnectionSettingsBuilder.Create()
            .Uri(new Uri(_brokerUri))
            .ContainerId("tutorial-newtask")
            .Build();

        IEnvironment environment = AmqpEnvironment.Create(settings);

        IConnection connection = await environment.CreateConnectionAsync();

        try
        {
            IManagement management = connection.Management();

            IQueueSpecification queueSpecification = management.Queue(_queueName).Type(QueueType.QUORUM);
            await queueSpecification.DeclareAsync();

            IPublisher publisher = await connection.PublisherBuilder().Queue(_queueName).BuildAsync();

            try
            {
                AmqpMessage amqpMessage = new(Encoding.UTF8.GetBytes(message));

                PublishResult pr = await publisher.PublishAsync(amqpMessage);

                switch (pr.Outcome.State)
                {
                    case OutcomeState.Accepted:
                        Console.WriteLine($" [x] Sent '{message}'");
                        break;
                    case OutcomeState.Released:
                        Console.Error.WriteLine($"Released message: {pr.Message.BodyAsString()}");
                        break;
                    case OutcomeState.Rejected:
                        Console.Error.WriteLine($"[Publisher] Message: {pr.Message.BodyAsString()} rejected with error: {pr.Outcome.Error}");
                        break;
                    default:
                        Console.Error.WriteLine($"Unexpected publish outcome: {pr.Outcome.State}");
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


