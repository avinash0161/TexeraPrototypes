using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans.Streams;

namespace TexeraOrleansPrototype
{
    public class StreamObserver : IAsyncObserver<Tuple>
    {
        // private ILogger logger;
        // public StreamObserver(ILogger logger)
        // {
           // this.logger = logger;
        // }

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

        public Task OnNextAsync(Tuple item, StreamSequenceToken token = null)
        {
            Console.WriteLine($"=={item.id}== received: by client");
            return Task.CompletedTask;
        }
    }
}