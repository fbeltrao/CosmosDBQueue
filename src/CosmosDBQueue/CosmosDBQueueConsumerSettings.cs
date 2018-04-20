namespace CosmosDBQueue
{
    /// <summary>
    /// Queue consumer settings
    /// </summary>
    public class CosmosDBQueueConsumerSettings
    {
        /// <summary>
        /// Defines the collection where queue items are placed
        /// </summary>
        public CosmosDBCollectionDefinition QueueCollectionDefinition { get; set; }

        /// <summary>
        /// Defines the collection where cosmosdb change feed lease is stored
        /// </summary>
        public CosmosDBCollectionDefinition LeaseCollectionDefinition { get; set; }

        /// <summary>
        /// Timeout to indicate that a queue item in processing timed out, being added to the queue of open items again
        /// </summary>
        public int ProcessingItemTimeout { get; set; } = 60;

        /// <summary>
        /// Current worker id
        /// Default is "default"
        /// </summary>
        public string WorkerId { get; set; } = "default";

        /// <summary>
        /// Indicates if queue item processing should be single threaded or in parallel
        /// Default: false (in parallel)
        /// </summary>
        public bool SingleThreadedProcessing { get; set; } = false;

        /// <summary>
        /// Indicates if queue items should be set to completed automatically by the consumer
        /// Default: true
        /// </summary>
        public bool AutoComplete { get; set; } = true;
    }
}
