using RPCClient;

public class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("Sending number..");

        int n = 30;
        if (args.Length > 0 && int.TryParse(args[0], out int input))
        {
            n = input;
        }

        int result = await SendRequestAsync(
            n, 
            queueName: "rpc_queue", 
            hostName: "localhost");

        Console.WriteLine(" [.] Got '{0}'", result);

        Console.WriteLine(" Press [enter] to exit.");
        Console.ReadLine();
    }

    private static async Task<int> SendRequestAsync(int n, string queueName, string hostName)
    {
        await using RpcClient rpcClient = new(queueName, hostName);

        await rpcClient.StartAsync();

        Console.WriteLine($" [x] Requesting fib({n})");

        int response = await rpcClient.CallAsync(n);

        return response;
    }
}
