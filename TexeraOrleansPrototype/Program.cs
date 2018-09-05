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
        public const int num_scan = 1;
        static async Task Main(string[] args)
        {
            if (args.Length < 1) return;
            if (args[0] == "silo")
            {
                const string connectionString = "server=texera-test1;uid=root;pwd=pwd;database=orleans;SslMode=none";
                var siloBuilder = new SiloHostBuilder()
                 .UseAdoNetClustering(options =>
                 {
                     options.ConnectionString = connectionString;
                     options.Invariant = "MySql.Data.MySqlClient";
                 })
                .AddSimpleMessageStreamProvider("SMSProvider")
                .AddMemoryGrainStorage("PubSubStore")
                .UseDashboard(options => { })
                .Configure<ClusterOptions>(options =>
                {
                    options.ClusterId = "dev";
                    options.ServiceId = "TexeraOrleansPrototype";
                })
                .ConfigureEndpoints(siloPort: 11111, gatewayPort: 30000)
                .ConfigureLogging(logging => logging.SetMinimumLevel(LogLevel.Critical).AddConsole());
                var host = siloBuilder.Build();
                await host.StartAsync();
                Console.WriteLine("Silo Started!");
                Console.ReadLine();
            }
            else if (args[0] == "server")
            {
                const string connectionString = "server=texera-test1;uid=root;pwd=pwd;database=orleans;SslMode=none";
                var clientBuilder = new ClientBuilder()
                    .UseAdoNetClustering(options =>
                    {
                        options.ConnectionString = connectionString;
                        options.Invariant = "MySql.Data.MySqlClient";
                    })
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
                await so.Start();
                for (int i = 0; i < num_scan; ++i)
                    client.GetGrain<IScanOperator>(i + 2).SubmitTuples();
                Console.ReadLine();
            }
            else
            {
                Console.WriteLine("Invaild Command!");
            }
        }

       
    }
}
