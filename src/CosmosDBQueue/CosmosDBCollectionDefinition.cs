using System;
using System.Collections.Generic;
using System.Text;

namespace CosmosDBQueue
{
    public class CosmosDBCollectionDefinition
    {
        public string Endpoint { get; set; }
        public string SecretKey { get; set; }
        public string DbName { get; set; }
        public string CollectionName { get; set; }
        public int Throughput { get; set; }
    }
}
