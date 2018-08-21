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
        private static int num_scan = 10;
        static async Task Main(string[] args)
        {
            if (args[0] == "c")
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
                .ConfigureLogging(logging => logging.SetMinimumLevel(LogLevel.Critical).AddConsole());
                var host = siloBuilder.Build();
                await host.StartAsync();
                Console.WriteLine("Silo Started!");
                Console.ReadLine();
            }
            else if (args[0] == "s")
            {
                var clientBuilder = new ClientBuilder()
                    .UseLocalhostClustering()
                    .AddSimpleMessageStreamProvider("SMSProvider")
                    .Configure<ClusterOptions>(options =>
                    {
                        options.ClusterId = "dev";
                        options.ServiceId = "TexeraOrleansPrototype";
                    })
                    .ConfigureLogging(logging => logging.AddConsole());
                var client = clientBuilder.Build();
                await client.Connect();
                Console.WriteLine("Server Started!");
                Guid streamGuid = await client.GetGrain<ICountOperator>(1).GetStreamGuid();

                Console.WriteLine("Client side guid is " + streamGuid);
                var stream = client.GetStreamProvider("SMSProvider")
                .GetStream<int>(streamGuid, "Random");
                var so = new StreamObserver();
                await stream.SubscribeAsync(so);

                System.IO.StreamReader file = new System.IO.StreamReader(@"d:\small_input.csv");
                int count = 0;
                bool need_break = false;
                List<IScanOperator> operators = new List<IScanOperator>();
                for (int i = 0; i < num_scan; ++i)
                {
                    var t = client.GetGrain<IScanOperator>(i + 2);
                    t.WakeUp();
                    operators.Add(t);
                    client.GetGrain<IFilterOperator>(i + 2).WakeUp();
                    client.GetGrain<IKeywordSearchOperator>(i + 2).WakeUp();
                    client.GetGrain<ICountOperator>(i + 2).WakeUp();
                }
                Thread.Sleep(1000);
                await so.Start();
                while (true)
                {
                    string line;
                    for (int i = 0; i < num_scan; ++i)
                    {
                        if ((line = file.ReadLine()) != null)
                        {
                            operators[i].SubmitTuples(new List<Tuple> { new Tuple(count, line.Split(",")) });
                            count++;
                        }
                        else
                            need_break = true;
                    }

                    if (need_break)
                    {
                        for (int i = 0; i < num_scan; ++i)
                            operators[i].SubmitTuples(new List<Tuple> { new Tuple(-1, null) });
                        break;
                    }
                }
                Console.WriteLine(count + "rows sent");
                Console.ReadLine();
            }
        }
    }
}
