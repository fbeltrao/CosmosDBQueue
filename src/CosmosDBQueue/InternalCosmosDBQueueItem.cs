using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System;
using System.Collections.Generic;
using System.Text;

namespace CosmosDBQueue
{
    public class InternalCosmosDBQueueItem
    {
        public string id { get; set; }

        [JsonConverter(typeof(StringEnumConverter))]
        public QueueItemStatus status { get; set; }

        public long queuedTime { get; set; }

        public long processStartTime { get; set; }

        public long completedTime { get; set; }
        public string currentWorker { get; set; }


        public long workerExpires { get; set; }

        public object data { get; set; }

        [JsonIgnore]
        public string etag { get; set; }

        public int errors { get; set; }

        public InternalCosmosDBQueueItem SetWorkerExpiration(int minutes)
        {
            this.workerExpires = Utils.ToUnixTime(DateTime.UtcNow.AddSeconds(minutes));
            return this;
        }

        public InternalCosmosDBQueueItem SetWorker(string workerId)
        {
            this.currentWorker = workerId;
            return this;
        }

        public InternalCosmosDBQueueItem SetProcessStartTime()
        {
            this.processStartTime = Utils.ToUnixTime(DateTime.UtcNow);
            return this;
        }

        public InternalCosmosDBQueueItem()
        {
            this.status = QueueItemStatus.Pending;
            this.queuedTime = Utils.ToUnixTime(DateTime.UtcNow);            
        }

        public void SetAsComplete()
        {
            this.status = QueueItemStatus.Completed;
            this.workerExpires = 0;
            this.completedTime = Utils.ToUnixTime(DateTime.UtcNow);
        }

        public void SetAsPending()
        {
            this.status = QueueItemStatus.Pending;
            this.workerExpires = 0;
            this.currentWorker = null;
            this.processStartTime = 0;
        }
    }
}
