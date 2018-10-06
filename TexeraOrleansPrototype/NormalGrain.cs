using Orleans;
using Orleans.Streams;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace TexeraOrleansPrototype
{
    public class NormalGrain : Grain, INormalGrain
    {
        private ulong current_seq_num = 0;
        public IAsyncStream<object> next_op = null;
        public IAsyncStream<object> current_op;
        public async override Task OnActivateAsync()
        {
            await current_op.SubscribeAsync(this);
            await base.OnActivateAsync();
        }

        public Task OutTo(string operator_name, bool empty_guid=false)
        {
            var streamProvider = GetStreamProvider("SMSProvider");
            next_op = streamProvider.GetStream<object>((empty_guid ? Guid.Empty : this.GetPrimaryKey()), operator_name);
            return Task.CompletedTask;
        }


        public virtual Task Process_impl(ref object row)
        {
            Console.WriteLine("OrderingGrain Process: " + row);
            return Task.CompletedTask;
        }

        public Task OnNextAsync(object obj, StreamSequenceToken token = null)
        {
            Process_impl(ref obj);
            if (obj != null)
            {
                if (next_op is IOrderingGrain)
                    (obj as Tuple).seq_token = current_seq_num++;
                if (next_op != null)
                    next_op.OnNextAsync(obj);
            }
            return Task.CompletedTask;
        }

        public Task OnCompletedAsync()
        {
            throw new NotImplementedException();
        }

        public Task OnErrorAsync(Exception ex)
        {
            throw new NotImplementedException();
        }
    }
}
