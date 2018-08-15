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

        public string output_operator;

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
            if (complete_count == 1)
                out_stream.OnNextAsync(count);
            return Task.CompletedTask;
        }

        public Task OnErrorAsync(Exception ex)
        {
            throw new NotImplementedException();
        }

        public Task OnNextAsync(int item, StreamSequenceToken token = null)
        {
            count += item;
            complete_count++;
            if (complete_count == 1)
                out_stream.OnNextAsync(count);
            return Task.CompletedTask;
        }
    }
}
