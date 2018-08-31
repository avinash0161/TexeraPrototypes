using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans.Streams;
using System.Diagnostics;

namespace TexeraOrleansPrototype
{
    public class StreamObserver : IAsyncObserver<int>
    {
<<<<<<< HEAD
        // private ILogger logger;
        private int id;
         public StreamObserver(int id)
         {
            this.id = id;
         }
=======
        private Stopwatch sw=new Stopwatch();

        public Task Start()
        {
            sw.Start();
            return Task.CompletedTask;
        }

>>>>>>> 0897f76f0dfcb5a6b5f7f075196b5f102cc381c3

        public Task OnCompletedAsync()
        {
            Console.WriteLine("Chatroom message stream received stream completed event");
            return Task.CompletedTask;
        }

        public Task OnErrorAsync(Exception ex)
        {
            Console.WriteLine($"Chatroom is experiencing message delivery failure, ex :{ex}");
            return Task.CompletedTask;
        }

        public Task OnNextAsync(int item, StreamSequenceToken token = null)
        {
<<<<<<< HEAD
            Console.WriteLine($"=={item}== count received: by client "+this.id);
=======
            sw.Stop();
            Console.WriteLine("Time usage: "+sw.Elapsed);
            Console.WriteLine($"=={item}== count received: by client");
>>>>>>> 0897f76f0dfcb5a6b5f7f075196b5f102cc381c3
            return Task.CompletedTask;
        }
    }
}