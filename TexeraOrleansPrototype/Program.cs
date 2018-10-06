
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using System;
using System.Net;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;

namespace TexeraOrleansPrototype
{
    class Program
    {
        public const int num_scan = 10;
        public const bool conditions_on = false;
        public const bool ordered_on = false;
        public const string dataset = "median"; // should be 'medium', but anyway...
        static async Task Main(string[] args)
        {
            var siloBuilder = new SiloHostBuilder()
                .UseLocalhostClustering()
                .AddSimpleMessageStreamProvider("SMSProvider")
                // add storage to store list of subscriptions
                .AddMemoryGrainStorage("PubSubStore")
                .UseDashboard(options => { })
                .Configure<ClusterOptions>(options =>
                {
                    options.ClusterId = "dev";
                    options.ServiceId = "TexeraOrleansPrototype";
                })
                .Configure<EndpointOptions>(options =>
                    options.AdvertisedIPAddress = IPAddress.Loopback)
                .ConfigureLogging(logging => logging.SetMinimumLevel(LogLevel.Critical).AddConsole())
                .Configure<MessagingOptions>(options => { options.ResendOnTimeout = true; options.MaxResendCount = 60; });
            using (var host = siloBuilder.Build())
            {
                await host.StartAsync();

                var clientBuilder = new ClientBuilder()
                    .UseLocalhostClustering()
                    .AddSimpleMessageStreamProvider("SMSProvider")
                    .Configure<ClusterOptions>(options =>
                    {
                        options.ClusterId = "dev";
                        options.ServiceId = "TexeraOrleansPrototype";
                    })
                    .ConfigureLogging(logging => logging.AddConsole());

                using (var client = clientBuilder.Build())
                {
                    await client.Connect();
                    Guid streamGuid = Guid.Empty;
                    var streamProvider = client.GetStreamProvider("SMSProvider");
                    var stream = streamProvider.GetStream<int>(streamGuid, "Random");
                    var so = new StreamObserver();
                    await stream.SubscribeAsync(so);

                    Console.WriteLine();
                    Console.WriteLine("Configuration:");
                    Console.WriteLine("# of workflows: " + Program.num_scan);
                    Console.WriteLine("FIFO & exactly-once: " + Program.ordered_on);
                    Console.WriteLine("dataset: " + Program.dataset);
                    Console.WriteLine("with conditions: " + Program.conditions_on);
                    Console.WriteLine();

                    List<IScanOperator> operators = new List<IScanOperator>();
                    for (int i = 0; i < num_scan; ++i)
                    {
                        var t = client.GetGrain<IScanOperator>(i + 2);
                        operators.Add(t);
                        if (ordered_on)
                        {
                            await client.GetGrain<IOrderedFilterOperator>(i + 2).OutTo("OrderedKeywordSearch");
                            await client.GetGrain<IOrderedKeywordSearchOperator>(i + 2).OutTo("OrderedCount");
                            await client.GetGrain<IOrderedCountOperator>(i + 2).OutTo("CountFinal", true);
                        }
                        else
                        {
                            await client.GetGrain<IFilterOperator>(i + 2).OutTo("KeywordSearch");
                            await client.GetGrain<IKeywordSearchOperator>(i + 2).OutTo("Count");
                            await client.GetGrain<ICountOperator>(i + 2).OutTo("CountFinal", true);
                        }
                        await client.GetGrain<ICountFinalOperator>(1).OutTo("Random", true);
                    }
                    Thread.Sleep(1000);
                    Console.WriteLine("Start loading tuples");
                    for (int i = 0; i < num_scan; ++i)
                        await operators[i].LoadTuples();
                    Console.WriteLine("Finish loading tuples");
                    await so.Start();
                    Console.WriteLine("Start experiment");
                    for (int i = 0; i < num_scan; ++i)
                    {
                        operators[i].SubmitTuples();
                    }
                    Console.ReadLine();
                    
                }
            }
        }

       
    }
}
