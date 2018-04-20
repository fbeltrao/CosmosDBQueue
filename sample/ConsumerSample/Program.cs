using CosmosDBQueue;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace ConsumerSample
{
    class Program
    {
        static void ConfigureServices(ServiceCollection services)
        {
            services
                .AddLogging(configure => configure.AddConsole())
                .Configure<LoggerFilterOptions>(options => options.MinLevel = LogLevel.Trace)
                .AddSingleton<CosmosDBQueueConsumer>()
                ;
        }

        static async Task Main(string[] args)
        {

            var services = new ServiceCollection();
            ConfigureServices(services);

            var serviceProvider = services.BuildServiceProvider();

            var consumer = serviceProvider.GetRequiredService<CosmosDBQueueConsumer>();            
            var random = new Random();

            consumer.OnMessage = async (string message) =>
            {
                await Task.Delay(10);
                var sample = random.Next(100) + 1;

                if (sample <= 5)
                {
                    throw new Exception("boom");
                }
                else if (sample <= 20)
                {
                    return false;
                }

                return true;
            };

            var settings = new CosmosDBQueueConsumerSettings()
            {
                LeaseCollectionDefinition = new CosmosDBCollectionDefinition
                {
                    Endpoint = "https://localhost:8081",
                    SecretKey = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==",
                    CollectionName = "lease",
                    Throughput = 400,
                    DbName = "queue",
                },

                QueueCollectionDefinition = new CosmosDBCollectionDefinition
                {
                    Endpoint = "https://localhost:8081",
                    SecretKey = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==",
                    CollectionName = "queue",
                    Throughput = 1000,
                    DbName = "queue",
                },

            };

            if (Debugger.IsAttached)
                settings.SingleThreadedProcessing = true;

            if (args.Length > 0)
                settings.WorkerId = args[0];
            else
                settings.WorkerId = "default";


            using (var cts = new CancellationTokenSource())
            {
                await consumer.Start(settings, cts);

                Console.WriteLine("Started, press a key to stop");
                Console.ReadLine();

                cts.Cancel();
            }

        }

    
    }
}
