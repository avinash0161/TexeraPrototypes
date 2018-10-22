
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
        static async Task Main(string[] args)
        {
            var siloBuilder = new SiloHostBuilder()
                .UseLocalhostClustering()
                .UseDashboard(options => {options.Port = 8086; })
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
                    var so = new ClientStreamObserver();
                    await stream.SubscribeAsync(so);

                    Console.WriteLine();
                    Console.WriteLine("Configuration:");
                    Console.WriteLine("Delivery: " + TexeraConfig.delivery);
                    Console.WriteLine("# of workflows: " + TexeraConfig.num_scan);
                    Console.WriteLine("FIFO & exactly-once: " + TexeraConfig.ordered_on);
                    Console.WriteLine("dataset: " + TexeraConfig.dataset);
                    Console.WriteLine("with conditions: " + TexeraConfig.conditions_on);
                    Console.WriteLine("Number of grains for each operator: " + TexeraConfig.num_scan);
                    Console.WriteLine();

                    List<IScanOperator> operators = new List<IScanOperator>();
                    for (int i = 0; i < TexeraConfig.num_scan; ++i)
                    {
                        var t = client.GetGrain<IScanOperator>(i + 2);
                        operators.Add(t);
                        if (TexeraConfig.ordered_on)
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
                    for (int i = 0; i < TexeraConfig.num_scan; ++i)
                        await operators[i].LoadTuples();
                    Console.WriteLine("Finish loading tuples");
                    await so.Start();
                    Console.WriteLine("Start experiment");
                    for (int i = 0; i < TexeraConfig.num_scan; ++i)
                    {
                          operators[i].SubmitTuples();
                    }
                    Console.ReadLine();
                    
                }
            }
        }

       
    }
}
