using Microsoft.Extensions.Logging;
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

                    System.IO.StreamReader file = new System.IO.StreamReader(@"d:\small_input.csv");
                    int count = 0;
                    List<IScanOperator> operators = new List<IScanOperator>();
                    for (int i = 0; i < num_scan; ++i)
                        operators.Add(client.GetGrain<IScanOperator>(i + 2));
                    while (true)
                    {
                        Console.WriteLine("Client giving another request");
                        // sensor.SubmitTuples(rows);
                        string line;
                        for (int i = 0; i <num_scan; ++i)
                        {
                            if ((line = file.ReadLine()) != null)
                            {
                                operators[i].SubmitTuples(new List<Tuple> { new Tuple(count, line.Split(",")) });
                                count++;
                            }
                            else
                            {
                                operators[i].QuitOperator();
                                count = -1;
                            }
                            Thread.Sleep(100);
                        }

                        // await t;
                        // Console.WriteLine("Client Task Status - "+t.Status);
                        Thread.Sleep(100);
                        Console.WriteLine("--------------------------");
                        if (count == -1)
                            break;
                    }
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
    }
}
