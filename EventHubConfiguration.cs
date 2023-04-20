namespace OTTODataInterface;

public class EventHubConfiguration
{
    public string StorageAccountConnectionString { get; set; }
    public string CheckpointContainerName { get; set; }
    public string ConsumerGroup { get; set; }
    public string IotEventHubConnectionString { get; set; }
}