// See https://aka.ms/new-console-template for more information

using Azure.Messaging.EventHubs.Primitives;
using Azure.Storage.Blobs;
using Microsoft.Extensions.Configuration;
using OTTODataInterface;

Console.WriteLine("Hello, World!");
var configuration =  new ConfigurationBuilder()
    .AddJsonFile($"appsettings.json")
    .Build();
            
var eventHubConf = configuration.GetSection("EventHubConf").Get<EventHubConfiguration>();
var storageClient  = new BlobContainerClient(eventHubConf.StorageAccountConnectionString, eventHubConf.CheckpointContainerName);
storageClient.CreateIfNotExists();

try
{
    var simpleBatchProcessor = new SimpleBatchProcessor(new BlobCheckpointStore(storageClient), 100, eventHubConf.ConsumerGroup, eventHubConf.IotEventHubConnectionString);
    await simpleBatchProcessor.StartProcessingAsync();
}
catch (Exception e)
{
    Console.WriteLine(e.Message);
}

Console.ReadKey();

