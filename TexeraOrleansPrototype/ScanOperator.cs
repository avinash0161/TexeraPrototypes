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
    public class ScanOperator : Grain, IScanOperator
    {

        public Orleans.Streams.IAsyncStream<List<Tuple>> in_stream;
        public Orleans.Streams.IAsyncStream<Tuple> out_stream;


        public Task OutTo(string operator_name)
        {
            var streamProvider = GetStreamProvider("SMSProvider");
            out_stream = streamProvider.GetStream<Tuple>(this.GetPrimaryKey(), operator_name);
            return Task.CompletedTask;
        }


        public async override Task OnActivateAsync()
        {
            var streamProvider = GetStreamProvider("SMSProvider");
            in_stream = streamProvider.GetStream<List<Tuple>>(this.GetPrimaryKey(), "Scan");
            await in_stream.SubscribeAsync(this);
            await base.OnActivateAsync();
            //Console.WriteLine("Scan: init");
        }

        public Task OnCompletedAsync()
        {
            //Console.WriteLine("Scan: END");
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

        public async Task OnNextAsync(List<Tuple> item, StreamSequenceToken token = null)
        {
            foreach(var i in item)
            {
                //Console.WriteLine("Scan: " + i.id);
                if(i.id==0)
                    await out_stream.OnNextAsync(i);
                else
                    out_stream.OnNextAsync(i);
            }
            return;
        }
    }
}