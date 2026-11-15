using RabbitMQ.Client;
using System.Text;


Console.WriteLine("Sending...");


ConnectionFactory factory = new() { HostName = "localhost" };
using IConnection connection = await factory.CreateConnectionAsync();
using IChannel channel = await connection.CreateChannelAsync();


await channel.QueueDeclareAsync(
    queue: "hello",
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: new Dictionary<string, object?> { { "x-queue-type", "quorum" } });


const string message = "Hello World 0.9.1!";
byte[] body = Encoding.UTF8.GetBytes(message);

await channel.BasicPublishAsync(exchange: string.Empty, routingKey: "hello", body: body);

Console.WriteLine($" [x] Sent {message}");
Console.WriteLine(" Press [enter] to exit.");
Console.ReadLine();
