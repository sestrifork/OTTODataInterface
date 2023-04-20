using System.Text;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Primitives;
using Newtonsoft.Json;

namespace OTTODataInterface;

public class SimpleBatchProcessor : PluggableCheckpointStoreEventProcessor<EventProcessorPartition>
{
    // This example uses a connection string, so only the single constructor
    // was implemented; applications will need to shadow each constructor of
    // the EventProcessorClient that they are using.

    public SimpleBatchProcessor(CheckpointStore checkpointStore, int eventBatchMaximumCount, string consumerGroup, string connectionString)
        : base(checkpointStore, eventBatchMaximumCount, consumerGroup, connectionString)
    {
    }

    protected override async Task OnProcessingEventBatchAsync(IEnumerable<EventData> events, EventProcessorPartition partition, CancellationToken cancellationToken)
    {
        // Like the event handler, it is very important that you guard
        // against exceptions in this override; the processor does not
        // have enough understanding of your code to determine the correct
        // action to take.  Any exceptions from this method go uncaught by
        // the processor and will NOT be handled.  The partition processing
        // task will fault and be restarted from the last recorded checkpoint.
        EventData lastEvent;
        try
        {
            foreach (var msg in events)
            {
                var data = Encoding.UTF8.GetString(msg.Body.ToArray());
                var dataObj = JsonConvert.DeserializeObject<dynamic>(data);
                Console.WriteLine(dataObj);
                // DO SOMETHING WITH THE DATA
            }

            Console.WriteLine($"Processed batch with {events.Count()} events from partition {partition.PartitionId}");
            if (events.Any())
            {
                lastEvent = events.Last();
                await UpdateCheckpointAsync(
                       partition.PartitionId,
                       lastEvent.Offset,
                       lastEvent.SequenceNumber,
                       cancellationToken).ConfigureAwait(false);
            }
        }

        catch (Exception ex)
        {
            Console.WriteLine($"Exception occured while reading from partition {partition.PartitionId}.\nException: {ex.Message}");
        }

        // Calling the base would only invoke the process event handler and provide no
        // value; we will not call it here.
    }

    protected override Task OnProcessingErrorAsync(Exception exception,
                                                         EventProcessorPartition partition,
                                                         string operationDescription,
                                                         CancellationToken cancellationToken)
    {
        // Like the event handler, it is very important that you guard
        // against exceptions in this override; the processor does not
        // have enough understanding of your code to determine the correct
        // action to take.  Any exceptions from this method go uncaught by
        // the processor and will NOT be handled.  Unhandled exceptions will
        // not impact the processor operation but will go unobserved, hiding
        // potential application problems.
        try
        {
            if (partition != null)
            {
                Console.WriteLine($"Exception on partition {partition.PartitionId} while " +
                                  $"performing {operationDescription}: {exception}");
            }
            else
            {
                Console.WriteLine($"Exception while performing {operationDescription}: {exception}");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Exception while processing events: {ex}");
        }
        return Task.CompletedTask;
    }
}
