using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans.Streams;

namespace TexeraOrleansPrototype
{
    public class StreamObserver : IAsyncObserver<int>
    {
        // private ILogger logger;
        private int id;
         public StreamObserver(int id)
         {
            this.id = id;
         }

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
            Console.WriteLine($"=={item}== count received: by client "+this.id);
            return Task.CompletedTask;
        }
    }
}