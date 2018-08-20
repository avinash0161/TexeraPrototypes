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
    public class KeywordSearchOperator : Grain, IKeywordSearchOperator
    {

        public FileStream fs;
        public StreamWriter sw;
        public Orleans.Streams.IAsyncStream<Tuple> in_stream;
        public Orleans.Streams.IAsyncStream<Tuple> out_stream;

        public Task OutTo(string operator_name)
        {
            var streamProvider = GetStreamProvider("SMSProvider");
            out_stream = streamProvider.GetStream<Tuple>(this.GetPrimaryKey(), operator_name);
            return Task.CompletedTask;
        }

        public async override Task OnActivateAsync()
        {
            string path = "KeywordSearch_" + this.GetPrimaryKeyLong().ToString();
            fs = new FileStream(path, FileMode.Create);
            sw = new StreamWriter(fs);
            var streamProvider = GetStreamProvider("SMSProvider");
            in_stream = streamProvider.GetStream<Tuple>(this.GetPrimaryKey(), "KeywordSearch");
            await in_stream.SubscribeAsync(this);
            await base.OnActivateAsync();
        }

        public Task OnCompletedAsync()
        {
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

        public async Task OnNextAsync(Tuple item, StreamSequenceToken token = null)
        {
            //Console.WriteLine("KeywordSearch: " + item.id);
            var cond = true;// item.region.Contains("Asia");
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