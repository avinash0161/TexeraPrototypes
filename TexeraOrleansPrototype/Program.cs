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

                    Guid streamGuid = await client.GetGrain<ICountOperator>(1).GetStreamGuid();

                    Console.WriteLine("Client side guid is " + streamGuid);
                    var stream = client.GetStreamProvider("SMSProvider")
                    .GetStream<int>(streamGuid, "Random");

                    await stream.SubscribeAsync(new StreamObserver());

                    Task.Run(() => AcceptInputForPauseResume(client));

                    System.IO.StreamReader file = new System.IO.StreamReader(@"d:\median_input.csv");
                    int count = 0;
                    bool need_break = false;
                    List<IScanOperator> operators = new List<IScanOperator>();
                    for (int i = 0; i < num_scan; ++i)
                        operators.Add(client.GetGrain<IScanOperator>(i + 2));
                    Stopwatch sw = new Stopwatch();
                    sw.Start();
                    while (true)
                    {
                        //Console.WriteLine("Client giving another request");
                        // sensor.SubmitTuples(rows);
                        string line;
                        for (int i = 0; i <num_scan; ++i)
                        { 
                            if ((line = file.ReadLine()) != null)
                            {
                                await operators[i].SubmitTuples(new List<Tuple> { new Tuple(count, line.Split(",")) });
                                count++;
                            }
                            else
                                need_break = true;
                            // Thread.Sleep(100);
                        }

                        // await t;
                        // Console.WriteLine("Client Task Status - "+t.Status);
                        // Thread.Sleep(100);
                        //Console.WriteLine("--------------------------");
                        if (need_break)
                        {
                            for (int i = 0; i < num_scan; ++i)
                                await operators[i].SubmitTuples(new List<Tuple> { new Tuple(-1, null) });
                            break;
                        }
                    }
                    sw.Stop();
                    Console.WriteLine("Time usage: " + sw.Elapsed);
                    Console.WriteLine(count + "rows sent");
                    Console.ReadLine();
                    Console.WriteLine("Flushing the buffer and closing the filestreams...");
                    for (int i = 0; i < num_scan; ++i)
                        await client.GetGrain<IScanOperator>(i + 2).QuitOperator();
                    Console.WriteLine("Complete!");
                    Console.ReadLine();
                    Console.WriteLine("Opening and merging...");
                    Console.WriteLine("Report: Scan operator missed "+ReportMissing("Scan_",count)+" row(s)");
                    Console.WriteLine("Report: Filter operator missed " + ReportMissing("Filter_", count) + " row(s)");
                    Console.WriteLine("Report: Keyword operator missed " + ReportMissing("KeywordSearch_", count) + " row(s)");
                    Console.WriteLine("Report: Count operator missed " + ReportMissing("Count_", count) + " row(s)");
                    Console.WriteLine("Complete!");
                    Console.ReadLine();
                    
                }
            }
        }

        public static void AcceptInputForPauseResume(IClusterClient client)
        {
            while(true)
            {
                char input = Console.ReadKey().KeyChar;
                if (input == 'p')
                {
                    // Console.WriteLine("Pause Called");
                    for (int i = 0; i < num_scan; ++i)
                        client.GetGrain<IScanOperator>(i + 2).PauseOperator();
                }
                else if (input == 'r')
                {
                    // Console.WriteLine("Resume Called");
                    for (int i = 0; i < num_scan; ++i)
                        client.GetGrain<IScanOperator>(i + 2).ResumeOperator();
                }
            }
        }

        public static int ReportMissing(string prefix,int count)
        {
            int res = 0;
            bool[] l = new bool[count];
            for (int i = 2; i < num_scan+2; ++i)
            {
                System.IO.StreamReader file = new System.IO.StreamReader(prefix+i.ToString());
                string line;
                while ((line = file.ReadLine()) != null)
                {
                    int temp = int.Parse(line);
                    l[temp] = true;
                }
            }
            foreach(var i in l)
            {
                if (!i) res++;
            }
            return res;
        }
    }
}
