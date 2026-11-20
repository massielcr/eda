using PublisherConfirms;

public class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("Publishing Individually...");
        await SendIndividuallyAsync("Single message", "localhost");


        Console.WriteLine("Publishing In Batch...");


        Console.WriteLine("Asynchronous handling Publishing...");
    }    


    private static async Task SendIndividuallyAsync(string message, string hostName)
    {
        await using var publisher = new Publisher(hostName);
        await publisher.SendIndviduallyAsync(message);

        Console.ReadLine();
    }

    private static async Task SendInBatchAsync(string[] messages, string hostName)
    {

    }

    private static async Task HandlePublishConfirmsAsync(string message, string hostName)
    {

    }
}


