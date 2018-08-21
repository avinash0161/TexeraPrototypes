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
        private static int num_scan = 10;

        //https://stackoverflow.com/questions/2101777/creating-an-ipendpoint-from-a-hostname
        public static IPEndPoint GetIPEndPointFromHostName(string hostName, int port, bool throwIfMoreThanOneIP)
        {
            var addresses = System.Net.Dns.GetHostAddresses(hostName);
            if (addresses.Length == 0)
            {
                throw new ArgumentException(
                    "Unable to retrieve address from specified host name.",
                    "hostName"
                );
            }
            else if (throwIfMoreThanOneIP && addresses.Length > 1)
            {
                throw new ArgumentException(
                    "There is more that one IP address to the specified host.",
                    "hostName"
                );
            }
            return new IPEndPoint(addresses[0], port); // Port gets validated here.
        }

        static async Task Main(string[] args)
        {
            //const string connectionString = "server=localhost;uid=root;pwd=;database=orleans;SslMode=none";
            if (args[0] == "c")
            {
                var siloBuilder = new SiloHostBuilder()
                .UseDevelopmentClustering(null)
                .AddSimpleMessageStreamProvider("SMSProvider")
                // add storage to store list of subscriptions
                .AddMemoryGrainStorage("PubSubStore")
                .UseDashboard(options => { })
                .Configure<ClusterOptions>(options =>
                {
                    options.ClusterId = "dev";
                    options.ServiceId = "TexeraOrleansPrototype";
                })
                .Configure<EndpointOptions>(options=>
                {
                    // Port to use for Silo-to-Silo
                    options.SiloPort = 11111;
                    // Port to use for the gateway
                    options.GatewayPort = 30000;
                    // IP Address to advertise in the cluster
                    options.AdvertisedIPAddress = IPAddress.Parse("10.142.0.3");
                    // The socket used for silo-to-silo will bind to this endpoint
                    options.GatewayListeningEndpoint = new IPEndPoint(IPAddress.Any, 40000);
                    // The socket used by the gateway will bind to this endpoint
                    options.SiloListeningEndpoint = new IPEndPoint(IPAddress.Any, 50000);
                })
                .ConfigureLogging(logging => logging.SetMinimumLevel(LogLevel.Critical).AddConsole());
                var host = siloBuilder.Build();
                await host.StartAsync();
                Console.WriteLine("Silo Started!");
                Console.ReadLine();
            }
            else if (args[0] == "s")
            {
                var clientBuilder = new ClientBuilder()
                    .UseStaticClustering(new IPEndPoint[] { GetIPEndPointFromHostName("texera-test2",30000,false) })
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
