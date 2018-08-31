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
    public class FilterOperator : Grain, IFilterOperator
    {
        public Orleans.Streams.IAsyncStream<Tuple> in_stream;
        public Orleans.Streams.IAsyncStream<Tuple> out_stream;

        public Task OutTo(string operator_name)
        {
            var streamProvider = GetStreamProvider("SMSProvider");
            out_stream = streamProvider.GetStream<Tuple>(this.GetPrimaryKey(),operator_name);
            return Task.CompletedTask;
        }

        public async override Task OnActivateAsync()
        {
            var streamProvider = GetStreamProvider("SMSProvider");
            in_stream = streamProvider.GetStream<Tuple>(this.GetPrimaryKey(), "Filter");
            await in_stream.SubscribeAsync(this);
            await base.OnActivateAsync();
            //Console.WriteLine("Filter: init");
        }

        public Task OnCompletedAsync()
        {
            //Console.WriteLine("Filter: END");
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

        public async Task OnNextAsync(Tuple item, StreamSequenceToken token = null)
        {
            //Console.WriteLine("Filter: " + item.id);
            var cond = true;// item.unit_cost > 50;
            if (item.id == -1 || cond)
            {
                if (item.id == 0)
                    await out_stream.OnNextAsync(item);
                else
                    out_stream.OnNextAsync(item);
            }
            return;
        }
    }
}