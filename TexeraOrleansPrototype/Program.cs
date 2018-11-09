﻿using Microsoft.Extensions.Logging;
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
        public const bool ordered_on = true;
        public const string dataset = "median";
        public const string delivery = "RPC";
        public const string dir= @"D:/";
        static async Task Main(string[] args)
        {
            var siloBuilder = new SiloHostBuilder()
                .UseLocalhostClustering()
                .AddSimpleMessageStreamProvider("SMSProvider")
                // add storage to store list of subscriptions
                .AddMemoryGrainStorage("PubSubStore")
                .UseDashboard(options => {options.Port = 8086; })
                .Configure<ClusterOptions>(options =>
                {
                    options.ClusterId = "dev";
                    options.ServiceId = "TexeraOrleansPrototype";
                })
                .Configure<EndpointOptions>(options =>
                    options.AdvertisedIPAddress = IPAddress.Loopback)
                .ConfigureLogging(logging => logging.SetMinimumLevel(LogLevel.Critical).AddConsole())
                .Configure<SiloMessagingOptions>(options => { options.ResendOnTimeout = true; options.MaxResendCount = 2; });

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
                    .ConfigureLogging(logging => logging.AddConsole())
                    .Configure<ClientMessagingOptions>(options => { options.ResendOnTimeout = true; options.MaxResendCount = 2; });
                using (var client = clientBuilder.Build())
                {
                    await client.Connect();

                    Guid streamGuid = await client.GetGrain<IOrderedCountFinalOperator>(1).GetStreamGuid();

                    Console.WriteLine("Client side guid is " + streamGuid);
                    var stream = client.GetStreamProvider("SMSProvider")
                    .GetStream<int>(streamGuid, "Random");
                    var so = new StreamObserver();
                    await stream.SubscribeAsync(so);

                    Console.WriteLine();
                    Console.WriteLine("Configuration:");
                    Console.WriteLine("Delivery: " + delivery);
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

                        // Explicitly activating other grains
                        await client.GetGrain<IOrderedFilterOperator>(i+2).TrivialCall();
                        
                        await client.GetGrain<IOrderedKeywordSearchOperator>(i+2).TrivialCall();
                        
                        await client.GetGrain<IOrderedCountOperator>(i+2).TrivialCall();
                        
                        await client.GetGrain<IOrderedCountFinalOperator>(1).TrivialCall();
                        
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
