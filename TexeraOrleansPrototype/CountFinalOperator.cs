using Orleans;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Orleans.Streams;

namespace TexeraOrleansPrototype
{
    class CountFinalOperator : Grain, ICountFinalOperator
    {
        public Orleans.Streams.IAsyncStream<int> in_stream;
        public Orleans.Streams.IAsyncStream<int> out_stream;

        private int count = 0;
        private int complete_count = 0;

        public Task OutTo(string operator_name)
        {
            var streamProvider = GetStreamProvider("SMSProvider");
            out_stream = streamProvider.GetStream<int>(Guid.Empty, operator_name);
            return Task.CompletedTask;
        }


        public async override Task OnActivateAsync()
        {
            var streamProvider = GetStreamProvider("SMSProvider");
            in_stream = streamProvider.GetStream<int>(Guid.Empty, "CountFinal");
            await in_stream.SubscribeAsync(this);
            await base.OnActivateAsync();
        }


        public Task OnCompletedAsync()
        {
            complete_count++;
<<<<<<< HEAD
            if (complete_count == 1)
                out_stream.OnNextAsync(count);
=======
            if (complete_count == Program.num_scan)
                {
                    out_stream.OnNextAsync(count);
                }
>>>>>>> 0897f76f0dfcb5a6b5f7f075196b5f102cc381c3
            return Task.CompletedTask;
        }

        public Task OnErrorAsync(Exception ex)
        {
            throw new NotImplementedException();
        }

        public Task OnNextAsync(int item, StreamSequenceToken token = null)
        {
            count += item;
            return Task.CompletedTask;
        }
    }
}
