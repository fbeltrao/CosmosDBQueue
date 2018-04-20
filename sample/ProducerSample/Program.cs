using CosmosDBQueue;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace ProducerSample
{
    class Program
    {
        static void ConfigureServices(ServiceCollection services)
        {
            services
                .AddLogging(configure => configure.AddConsole())
                .Configure<LoggerFilterOptions>(options => options.MinLevel = LogLevel.Trace)
                .AddSingleton<CosmosDBQueueProducer>()
                ;
        }

        static async Task Main(string[] args)
        {
            var services = new ServiceCollection();
            ConfigureServices(services);
            var serviceProvider = services.BuildServiceProvider();

            using (var producer = serviceProvider.GetRequiredService<CosmosDBQueueProducer>())
            {
                var settings = new CosmosDBQueueProducerSettings
                {
                    QueueCollectionDefinition = new CosmosDBCollectionDefinition
                    {
                        Endpoint = "https://localhost:8081",
                        SecretKey = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==",
                        CollectionName = "queue",
                        Throughput = 1000,
                        DbName = "queue",
                    },

                };
                await producer.Initialize(settings);

                var amountOfItems = 100;
                if (args.Length > 0)
                {
                    if (!int.TryParse(args[0], out amountOfItems))
                        amountOfItems = 100;
                }

                Parallel.For(0, amountOfItems, i =>
                {
                    try
                    {
                        producer.Queue(new { id = DateTime.UtcNow.Ticks }).GetAwaiter().GetResult();
                        Console.WriteLine($"Sent message {i + 1}");
                        Thread.Sleep(10);
                    }
                    catch (Exception ex)
                    {
                        Console.Error.WriteLine(ex.ToString());
                    }
                });

                if (Debugger.IsAttached)
                {
                    Console.WriteLine("Press enter to exit");
                    Console.ReadLine();
                }
            }
        }
    }
}
