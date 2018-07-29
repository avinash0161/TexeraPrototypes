using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using System;
using System.Net;
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

                    Guid streamGuid = await client.GetGrain<IKeywordSearchOperator>(500).GetStreamGuid();

                    Console.WriteLine("Client side guid is " + streamGuid);
                    var stream = client.GetStreamProvider("SMSProvider")
                    .GetStream<float>(streamGuid, "Random");
                    // await stream.SubscribeAsync(async (data, token) => Console.WriteLine(data));

                    await stream.SubscribeAsync(new StreamObserver());

                    while (true)
                    {
                        Console.WriteLine("Client giving another request");
                        
                        var sensor = client.GetGrain<IScanOperator>(500);
                        Task t = sensor.SubmitTuples((float)5.78);
                        // await t;
                        // Console.WriteLine("Client Task Status - "+t.Status);
                        Thread.Sleep(1000);
                    }
                }
            }
        }
    }
}
