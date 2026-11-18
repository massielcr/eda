using RPCServer;

public class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("Receiving requests..");

        await ProcessRequestAsync(
            queueName: "rpc_queue",
            hostName: "localhost");
    }

    private static async Task ProcessRequestAsync(string queueName, string hostName)
    {
        await using RpcServer rpcServer = new(queueName, hostName);

        await rpcServer.StartAsync();

        Console.WriteLine(" [x] Awaiting RPC requests");
        Console.WriteLine(" Press [enter] to exit.");
        Console.ReadLine();
    }
}
