using Microsoft.Azure.Documents.Client;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace CosmosDBQueue
{
    public class CosmosDBQueueProducer : IDisposable
    {
        private CosmosDBQueueProducerSettings settings;
        private readonly ILogger logger;
        DocumentClient queueCollectionClient;
        bool initialized = false;



        /// <summary>
        /// Constructor
        /// </summary>
        public CosmosDBQueueProducer()
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="logger"></param>
        public CosmosDBQueueProducer(ILogger<CosmosDBQueueProducer> logger)
        {            
            this.logger = logger;            
        }
       
        /// <summary>
        /// Initializes the queue
        /// </summary>
        /// <param name="settings"></param>
        public async Task Initialize(CosmosDBQueueProducerSettings settings)
        {
            if (this.initialized)
                throw new InvalidOperationException("Initialization already occured");

            this.settings = settings;
            await Utils.CreateCollectionIfNotExists(this.settings.QueueCollectionDefinition);

            this.queueCollectionClient = new DocumentClient(new Uri(this.settings.QueueCollectionDefinition.Endpoint), this.settings.QueueCollectionDefinition.SecretKey);

            this.initialized = true;            
        }


        /// <summary>
        /// Queue item returning queue item identifier
        /// </summary>
        /// <param name="payload"></param>
        /// <returns></returns>
        public async Task<string> Queue(object payload) => await Queue(null, payload);

        /// <summary>
        /// Queue item returning queue item identifier
        /// </summary>
        /// <param name="payload"></param>
        /// <returns></returns>
        public async Task<string> Queue(string id, object payload)
        {
            if (!this.initialized)            
                throw new InvalidOperationException("Must call Initialize() first");            

            var queueItem = new CosmosDBQueueItem()
            {
                id = id,
                data = payload
            };

            var uri = UriFactory.CreateDocumentCollectionUri(this.settings.QueueCollectionDefinition.DbName, this.settings.QueueCollectionDefinition.CollectionName);

            var res = await this.queueCollectionClient.CreateDocumentAsync(uri, queueItem);

            this.logger?.LogTrace($"Item {res.Resource.Id} queued, costs: {res.RequestCharge}, latency: {res.RequestLatency.TotalMilliseconds}ms");

            return res.Resource.Id;
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    this.queueCollectionClient?.Dispose();
                    this.queueCollectionClient = null;
                }

                disposedValue = true;
            }
        }
        
        public void Dispose()
        {           
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        #endregion
    }
}
