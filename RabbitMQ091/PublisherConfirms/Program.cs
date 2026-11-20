using PublisherConfirms;

public class Program
{
    public static async Task Main(string[] args)
    {
        const string hostName = "localhost";
        const int messageCount = 50;

        Console.WriteLine("Publishing Individually...");
        await SendIndividuallyAsync("Single message", hostName, messageCount);


        Console.WriteLine("Publishing In Batch...");
        await SendInBatchAsync("Batch message", hostName, messageCount);


        Console.WriteLine("Asynchronous handling Publishing...");
        await HandlePublishConfirmsAsync("Asynchronous handling", hostName, messageCount);
    }    


    private static async Task SendIndividuallyAsync(string message, string hostName, int count)
    {
        await using var publisher = new Publisher(hostName);
        await publisher.InitializedAsync();

        for (int i = 0; i < count; i++)
        {
            await publisher.SendIndviduallyAsync($"{message} - {i}");
        }

        Console.ReadLine();
    }

    private static async Task SendInBatchAsync(string message, string hostName, int count)
    {
        await using var publisher = new Publisher(hostName);
        await publisher.InitializedAsync();

        string[] messages = new string[count];
        for (int i = 0; i < count; i++)
        {
            messages[i] = $"{message} - {i}";
        }

        await publisher.SendInBatchAsync(messages);

        Console.ReadLine();
    }

    private static async Task HandlePublishConfirmsAsync(string message, string hostName, int count)
    {
        await using var publisher = new Publisher(hostName);
        await publisher.InitializedAsync();

        string[] messages = new string[count];
        for (int i = 0; i < count; i++)
        {
            messages[i] = $"{message} - {i}";
        }

        await publisher.HandlePublishConfirmsAsync(messages);

        Console.ReadLine();
    }
}


