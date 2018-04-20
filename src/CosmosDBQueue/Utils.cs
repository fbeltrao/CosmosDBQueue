using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace CosmosDBQueue
{
    static class Utils
    {
        static DateTime epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        internal static long ToUnixTime(DateTime date)
        {
            return Convert.ToInt64((date - epoch).TotalSeconds);
        }


        /// <summary>
        /// Checks whether collections exists. Creates new collection if collection does not exist 
        /// WARNING: CreateCollectionIfNotExistsAsync will create a new 
        /// with reserved throughput which has pricing implications. For details
        /// visit: https://azure.microsoft.com/en-us/pricing/details/cosmos-db/
        /// </summary>
        /// <param name="endPointUri">End point URI for account </param>
        /// <param name="secretKey">Primary key to access the account </param>
        /// <param name="databaseName">Name of database </param>
        /// <param name="collectionName">Name of collection</param>
        /// <param name="throughput">Amount of throughput to provision</param>
        /// <returns>A Task to allow asynchronous execution</returns>
        internal static async Task CreateCollectionIfNotExists(CosmosDBCollectionDefinition collectionDefinition)
        {
            // connecting client 
            using (var client = new DocumentClient(new Uri(collectionDefinition.Endpoint), collectionDefinition.SecretKey))
            {
                await client.CreateDatabaseIfNotExistsAsync(new Database { Id = collectionDefinition.DbName });

                // create collection if it does not exist 
                // WARNING: CreateDocumentCollectionIfNotExistsAsync will create a new 
                // with reserved throughput which has pricing implications. For details
                // visit: https://azure.microsoft.com/en-us/pricing/details/cosmos-db/
                await client.CreateDocumentCollectionIfNotExistsAsync(
                    UriFactory.CreateDatabaseUri(collectionDefinition.DbName),
                    new DocumentCollection { Id = collectionDefinition.CollectionName },
                    new RequestOptions { OfferThroughput = collectionDefinition.Throughput });
            }
        }

    }
}
