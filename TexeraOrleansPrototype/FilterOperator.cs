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
        public FileStream fs;
        public StreamWriter sw;
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
            string path = "Filter_" + this.GetPrimaryKeyLong().ToString();
            fs = new FileStream(path, FileMode.Create);
            sw = new StreamWriter(fs);
            var streamProvider = GetStreamProvider("SMSProvider");
            in_stream = streamProvider.GetStream<Tuple>(this.GetPrimaryKey(), "Filter");
            await in_stream.SubscribeAsync(this);
            await base.OnActivateAsync();
            Console.WriteLine("Filter: init");
        }

        public Task OnCompletedAsync()
        {
            Console.WriteLine("Filter: END");
            out_stream.OnCompletedAsync();
            return Task.CompletedTask;
        }

        public override Task OnDeactivateAsync()
        {
            sw.Flush();
            fs.Close();
            return base.OnDeactivateAsync();
        }

        public Task OnErrorAsync(Exception ex)
        {
            throw new NotImplementedException();
        }

        public Task OnNextAsync(Tuple item, StreamSequenceToken token = null)
        {
            Console.WriteLine("Filter: " + item.id);
            if (item.unit_cost > 50)
                out_stream.OnNextAsync(item);
            return Task.CompletedTask;
        }
    }
}