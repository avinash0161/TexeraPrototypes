﻿using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using System;
using System.Net;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace TexeraOrleansPrototype
{
    class Program
    {
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
                .ConfigureLogging(logging => logging.SetMinimumLevel(LogLevel.Critical).AddConsole());

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

                    List<Tuple> rows = new List<Tuple>();
                    rows.Add(new Tuple(1, "when it rains it pours", 34));
                    rows.Add(new Tuple(2, "the rains it raineth everyday", 4));
                    rows.Add(new Tuple(3, "if the aim is total abject embarrassment", 34));
                    rows.Add(new Tuple(4, "a star winked at me btwn the apricot", 34));
                    rows.Add(new Tuple(5, "it rains and pours", 34));

                    Guid streamGuid = await client.GetGrain<IKeywordSearchOperator>(500).GetStreamGuid();

                    Console.WriteLine("Client side guid is " + streamGuid);
                    var stream = client.GetStreamProvider("SMSProvider")
                    .GetStream<Tuple>(streamGuid, "Random");

                    await stream.SubscribeAsync(new StreamObserver());

                    while (true)
                    {
                        Console.WriteLine("Client giving another request");
                        
                        var sensor = client.GetGrain<IScanOperator>(500);

                        foreach(Tuple row in rows)
                        {
                            Task t = sensor.SubmitTuples(row);
                        }
                        
                        // await t;
                        // Console.WriteLine("Client Task Status - "+t.Status);
                        Thread.Sleep(10000);
                    }
                }
            }
        }
    }
}
