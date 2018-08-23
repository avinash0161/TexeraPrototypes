using Orleans;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.IO;
using Orleans.Streams;

namespace TexeraOrleansPrototype
{
    public class CountOperator : Grain, ICountOperator
    {
        private int count = 0;
        public Orleans.Streams.IAsyncStream<Tuple> in_stream;
        public Orleans.Streams.IAsyncStream<int> out_stream;


        public Task OutTo(string operator_name)
        {
            var streamProvider = GetStreamProvider("SMSProvider");
            out_stream = streamProvider.GetStream<int>(Guid.Empty, operator_name);
            return Task.CompletedTask;
        }


        public async override Task OnActivateAsync()
        {
            var streamProvider = GetStreamProvider("SMSProvider");
            in_stream = streamProvider.GetStream<Tuple>(this.GetPrimaryKey(), "Count");
            await in_stream.SubscribeAsync(this);
            await base.OnActivateAsync();
        }


        public Task OnCompletedAsync()
        {
            out_stream.OnNextAsync(count);
            out_stream.OnCompletedAsync();
            return Task.CompletedTask;
        }

        public override Task OnDeactivateAsync()
        {
            return base.OnDeactivateAsync();
        }

        public Task OnErrorAsync(Exception ex)
        {
            throw new NotImplementedException();
        }

        public Task OnNextAsync(Tuple item, StreamSequenceToken token = null)
        {
            if(item.id==-1)
                out_stream.OnNextAsync(count);
            else
                count++;
            return Task.CompletedTask;
        }
    }
}