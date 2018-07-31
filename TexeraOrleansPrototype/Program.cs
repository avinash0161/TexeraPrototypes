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

                    Guid streamGuid = await client.GetGrain<IKeywordSearchOperator>(0).GetStreamGuid();

                    Console.WriteLine("Client side guid is " + streamGuid);
                    var stream = client.GetStreamProvider("SMSProvider")
                    .GetStream<Tuple>(streamGuid, "Random");

                    await stream.SubscribeAsync(new StreamObserver());

                    Task.Run(() => AcceptInputForPauseResume(client));

                    System.IO.StreamReader file = new System.IO.StreamReader(@"d:\large_input.csv");
                    int count = 0;
                    while (true)
                    {
                        Console.WriteLine("Client giving another request");

                        List<IScanOperator> operators=new List<IScanOperator>();
                   
                        for(int i=0;i<5;++i)
                            operators.Add(client.GetGrain<IScanOperator>(i+1));
                        // sensor.SubmitTuples(rows);
                        string line;
                        for (int i = 0; i < 5; ++i)
                        {
                            if ((line = file.ReadLine()) != null)
                            {
                                operators[i].SubmitTuples(new List<Tuple>{ new Tuple(count,line.Split(","))});
                                count++;
                            }
                            Thread.Sleep(1000);
                        }

                        // await t;
                        // Console.WriteLine("Client Task Status - "+t.Status);
                        Thread.Sleep(5000);
                        Console.WriteLine("--------------------------");
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
                    IScanOperator  scan = client.GetGrain<IScanOperator>(500);
                    IFilterOperator filter = client.GetGrain<IFilterOperator>(500);
                    IKeywordSearchOperator keyword = client.GetGrain<IKeywordSearchOperator>(500);
                    
                    scan.PauseOperator();
                    filter.PauseOperator();
                    keyword.PauseOperator();
                }
                else if (input == 'r')
                {
                    // Console.WriteLine("Resume Called");
                    IScanOperator  scan = client.GetGrain<IScanOperator>(500);
                    IFilterOperator filter = client.GetGrain<IFilterOperator>(500);
                    IKeywordSearchOperator keyword = client.GetGrain<IKeywordSearchOperator>(500);
                    
                    scan.ResumeOperator();
                    filter.ResumeOperator();
                    keyword.ResumeOperator();
                }
            }
        }
    }
}
