using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.ChangeFeedProcessor;
using Microsoft.Azure.Documents.Client;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CosmosDBQueue
{
    /// <summary>
    /// Cosmos DB queue consumer
    /// </summary>
    public class CosmosDBQueueConsumer : IChangeFeedObserver, IChangeFeedObserverFactory
    {
        CosmosDBQueueConsumerSettings settings;
        readonly ILogger logger;
        string workerId;
        bool started = false;
        DocumentClient queueCollectionClient;


        /// <summary>
        /// Constructor
        /// </summary>
        public CosmosDBQueueConsumer()
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="logger"></param>
        public CosmosDBQueueConsumer(ILogger<CosmosDBQueueConsumer> logger)
        {
            this.logger = logger;
        }

        /// <summary>
        /// Sets callback function handling queue items
        /// </summary>
        public Func<CosmosDBQueueItem, Task<bool>> OnMessage { get; set; }

        IChangeFeedObserver IChangeFeedObserverFactory.CreateObserver() => this;
        Task IChangeFeedObserver.OpenAsync(ChangeFeedObserverContext context) => Task.CompletedTask;
        Task IChangeFeedObserver.CloseAsync(ChangeFeedObserverContext context, ChangeFeedObserverCloseReason reason) => Task.CompletedTask;

        /// <summary>
        /// Registers change feed observer to update changes read on change feed to destination 
        /// collection. Deregisters change feed observer and closes process when enter key is pressed
        /// </summary>
        /// <returns>A Task to allow asynchronous execution</returns>
        public async Task Start(CosmosDBQueueConsumerSettings settings, CancellationTokenSource cts = default)
        {
            if (this.started)
                throw new InvalidOperationException("Already started");

            this.settings = settings;
            await Utils.CreateCollectionIfNotExists(this.settings.QueueCollectionDefinition);
            await Utils.CreateCollectionIfNotExists(this.settings.LeaseCollectionDefinition);

            if (this.OnMessage == null)
                throw new InvalidOperationException($"Set a {nameof(OnMessage)} before start consuming queue items");

            this.workerId = this.settings.WorkerId ?? Guid.NewGuid().ToString();

            // queue collection info 
            var documentCollectionLocation = new DocumentCollectionInfo
            {
                Uri = new Uri(this.settings.QueueCollectionDefinition.Endpoint),
                MasterKey = this.settings.QueueCollectionDefinition.SecretKey,
                DatabaseName = this.settings.QueueCollectionDefinition.DbName,
                CollectionName = this.settings.QueueCollectionDefinition.CollectionName
            };

            // lease collection info 
            DocumentCollectionInfo leaseCollectionLocation = new DocumentCollectionInfo
            {
                Uri = new Uri(this.settings.LeaseCollectionDefinition.Endpoint),
                MasterKey = this.settings.LeaseCollectionDefinition.SecretKey,
                DatabaseName = this.settings.LeaseCollectionDefinition.DbName,
                CollectionName = this.settings.LeaseCollectionDefinition.CollectionName,
            };

            this.queueCollectionClient = new DocumentClient(new Uri(this.settings.QueueCollectionDefinition.Endpoint), this.settings.QueueCollectionDefinition.SecretKey);

            // Customizable change feed option and host options 
            ChangeFeedOptions feedOptions = new ChangeFeedOptions
            {
                StartFromBeginning = true
            };

            ChangeFeedHostOptions feedHostOptions = new ChangeFeedHostOptions
            {
                LeaseRenewInterval = TimeSpan.FromSeconds(15),
                LeasePrefix = string.Concat(this.workerId, "_"),                
            };

            this.logger?.LogTrace($"Starting worker {this.workerId}");

            ChangeFeedEventHost host = new ChangeFeedEventHost(this.workerId, documentCollectionLocation, leaseCollectionLocation, feedOptions, feedHostOptions);
            await host.RegisterObserverFactoryAsync(this);

            if (cts != null)
            {
                cts.Token.Register(async() =>
                {
                    await host.UnregisterObserversAsync();
                });
            }

            this.started = true;
        }


        /// <summary>
        /// Handles change feed documents called by host
        /// </summary>
        /// <param name="context"></param>
        /// <param name="docs"></param>
        /// <returns></returns>
        async Task IChangeFeedObserver.ProcessChangesAsync(ChangeFeedObserverContext context, IReadOnlyList<Document> docs)
        {
            var documents = new List<Document>();
            foreach (Document doc in docs)
            {
                var status = doc.GetPropertyValue<QueueItemStatus>("status");
                if (status == QueueItemStatus.Pending)
                {
                    documents.Add(doc);
                }
            }

            // Lock queue items
            var queueItems = await TryAdquireLock(documents);
            if (queueItems.Count > 0)
                await ProcessQueueItems(queueItems);                            
        }

        /// <summary>
        /// Tries to adquire lock in queue item to be the actual processor        
        /// </summary>
        /// <remarks>
        /// Successfully adquiring a lock means:
        ///     - Document was updated with { status: 'InProgress', workerId: '[this_worker_id]', processStartTime: currentTime }, if the ETag did not change (optimistic locking)
        /// </remarks>
        /// <param name="documents"></param>
        /// <returns></returns>
        private async Task<IList<InternalCosmosDBQueueItem>> TryAdquireLock(IEnumerable<Document> documents)
        {

            // TODO: Replace with stored procedure to batch lock items
            var result = new List<InternalCosmosDBQueueItem>();

            foreach (var doc in documents)
            {
                try
                {
                    this.logger?.LogTrace($"Trying to lock item {doc.Id}");

                    var updatedDocument = new InternalCosmosDBQueueItem
                    {
                        status = QueueItemStatus.InProgress,
                        data = doc.GetPropertyValue<object>("data"),
                        id = doc.Id,
                        processStartTime = doc.GetPropertyValue<long>(nameof(InternalCosmosDBQueueItem.processStartTime)),
                        queuedTime = doc.GetPropertyValue<long>(nameof(InternalCosmosDBQueueItem.queuedTime)),
                        errors = doc.GetPropertyValue<int>(nameof(InternalCosmosDBQueueItem.errors)),
                        completedTime = doc.GetPropertyValue<long>(nameof(InternalCosmosDBQueueItem.completedTime)),
                    };

                    updatedDocument.SetWorker(this.workerId).SetWorkerExpiration(settings.ProcessingItemTimeout).SetProcessStartTime();

                   
                    var res = await SaveQueueItem(updatedDocument);

                    this.logger?.LogTrace($"Queue item {updatedDocument.id} adquired lock succeeded, costs: {res.RequestCharge}, latency: {res.RequestLatency.TotalMilliseconds}ms");

                    var savedDocument = JsonConvert.DeserializeObject<InternalCosmosDBQueueItem>(res.Resource.ToString());
                    savedDocument.etag = res.ResponseHeaders["ETag"];
                    result.Add(savedDocument);
                }
                catch (DocumentClientException documentClientException)
                {
                    if (documentClientException.StatusCode != System.Net.HttpStatusCode.PreconditionFailed)
                    {
                        this.logger?.LogError(documentClientException, $"Error during queue item {doc.Id} adquired lock attempt");
                    }
                }
                catch (Exception ex)
                {
                    this.logger?.LogError(ex, $"Error during queue item {doc.Id} adquired lock attempt");
                }                
            }

            return result;
        }

        

        /// <summary>
        /// Process the queue items calling the <see cref="OnMessage"/> handler
        /// </summary>
        /// <param name="queueItems"></param>
        /// <returns></returns>
        private async Task ProcessQueueItems(IList<InternalCosmosDBQueueItem> queueItems)
        {
            if (this.settings.SingleThreadedProcessing)
            {                
                foreach (var queueItem in queueItems)
                {
                    await ProcessSingleItem(queueItem, persistDocument: true);
                }
            }
            else
            {
                Parallel.ForEach(queueItems, queueItem => ProcessSingleItem(queueItem, persistDocument: true).GetAwaiter().GetResult());

                // persist modified queue items
                // TODO: use stored procedure w/ batch update                
            }            
        }

        /// <summary>
        /// Processes a single queue item
        /// </summary>
        /// <param name="queueItem"></param>
        /// <param name="persistDocument"></param>
        /// <returns></returns>
        async Task ProcessSingleItem(InternalCosmosDBQueueItem queueItem, bool persistDocument)
        {
            // Persist document only if Auto complete is enabled
            persistDocument = persistDocument & this.settings.AutoComplete;

            try
            {
                var externalQueueItem = new CosmosDBQueueItem(this, queueItem);
                var handlerStatus = await OnMessage(externalQueueItem);
                if (handlerStatus)
                {
                    if (this.settings.AutoComplete)
                        queueItem.SetAsComplete();

                    this.logger?.LogTrace($"Processing item {queueItem.id} succeeded");

                }
                else
                {
                    if (this.settings.AutoComplete)
                    {
                        queueItem.SetAsPending();
                        queueItem.errors++;
                    }

                    this.logger?.LogTrace($"Processing item {queueItem.id} failed");
                }

            }
            catch (Exception ex)
            {
                this.logger?.LogError(ex, $"Error processing queue item {queueItem.id}");

                queueItem.errors++;
                queueItem.SetAsPending();

                // If the message handler failed we saved as failed
                persistDocument = true;
            }

            if (persistDocument)
            {
                await SaveQueueItem(queueItem);
            }
        }



        /// <summary>
        /// Completes the message queue
        /// </summary>
        /// <param name="queueItem"></param>
        /// <returns></returns>
        public async Task Complete(InternalCosmosDBQueueItem queueItem)
        {
            queueItem.SetAsComplete();
            await SaveQueueItem(queueItem);
        }


        /// <summary>
        /// Completes the message queue
        /// </summary>
        /// <param name="queueItem"></param>
        /// <returns></returns>
        public async Task Abandon(InternalCosmosDBQueueItem queueItem)
        {
            queueItem.SetAsPending();
            queueItem.errors++;
            await SaveQueueItem(queueItem);
        }

        async Task<ResourceResponse<Document>> SaveQueueItem(InternalCosmosDBQueueItem queueItem)
        {
            try
            {
                var uri = UriFactory.CreateDocumentCollectionUri(this.settings.QueueCollectionDefinition.DbName, this.settings.QueueCollectionDefinition.CollectionName);
                var reqOptions = new RequestOptions()
                {
                    AccessCondition = new AccessCondition()
                    {
                        Condition = queueItem.etag,
                        Type = AccessConditionType.IfMatch
                    }
                };

                var res = await queueCollectionClient.UpsertDocumentAsync(uri, queueItem, reqOptions);
                this.logger?.LogTrace($"Queue item {queueItem.id} updated, costs: {res.RequestCharge}, latency: {res.RequestLatency.TotalMilliseconds}ms");

                return res;
            }
            catch (Exception ex)
            {
                this.logger?.LogError(ex, $"Error persisting queue item {queueItem.id}");
            }

            return null;
        }
    }
}
