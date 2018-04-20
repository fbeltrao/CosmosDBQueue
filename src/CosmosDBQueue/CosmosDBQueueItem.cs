using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace CosmosDBQueue
{
    /// <summary>
    /// CosmosDB queue item used by processors/consumers
    /// </summary>
    public class CosmosDBQueueItem
    {
        private readonly CosmosDBQueueConsumer consumer;
        private readonly InternalCosmosDBQueueItem queueItem;

        /// <summary>
        /// Queue item data
        /// </summary>
        public object Data { get { return this.queueItem.data; } }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="consumer"></param>
        /// <param name="queueItem"></param>
        protected internal CosmosDBQueueItem(CosmosDBQueueConsumer consumer, InternalCosmosDBQueueItem queueItem)
        {
            this.consumer = consumer;
            this.queueItem = queueItem;
        }

        /// <summary>
        /// Complete the queue item, marking with status <see cref="QueueItemStatus.Completed"/>
        /// </summary>
        /// <returns></returns>
        public async Task Complete()
        {
            await this.consumer.Complete(queueItem);
        }

        /// <summary>
        /// Cancels the processing of the item, setting the status to <see cref="QueueItemStatus.Pending"/>
        /// </summary>
        /// <returns></returns>
        public async Task Abandon()
        {
            await this.consumer.Abandon(queueItem);
        }
    }
}
