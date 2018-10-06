//#define SHOW_STASHED 



using Orleans;
using Orleans.Streams;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace TexeraOrleansPrototype
{
    public class OrderingGrain: Grain, IOrderingGrain
    {
        private Dictionary<ulong, object> stashed = new Dictionary<ulong, object>();
        private ulong current_idx = 0;
        private ulong current_seq_num = 0;
        public IAsyncStream<object> next_op = null;
        public IAsyncStream<object> current_op;

        public async override Task OnActivateAsync()
        {
            await current_op.SubscribeAsync(this);
            await base.OnActivateAsync();
        }

        public Task OutTo(string operator_name,bool empty_guid=false)
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

        private void ProcessStashed()
        {
            while (true)
            {
                if (stashed.ContainsKey(current_idx))
                {
                    var obj = stashed[current_idx];
                    Process_impl(ref obj);
                    if (obj != null)
                    {
                        if (next_op is IOrderingGrain)
                            (obj as Tuple).seq_token = current_seq_num++;
                        if (next_op != null)
                            next_op.OnNextAsync(obj);
                    }
                    stashed.Remove(current_idx);
                    current_idx++;
                }
                else
                    break;
            }
        }

        public Task OnNextAsync(object obj, StreamSequenceToken token = null)
        {
            var seq_token = (obj as Tuple).seq_token;
            if (seq_token < current_idx)
            {
                // de-dup messages
                return Task.CompletedTask;
            }
            if (seq_token != current_idx)
            {
                stashed.Add(seq_token, obj);
#if SHOW_STASHED
                Console.WriteLine("Stashed " + seq_token + " !");
#endif
            }
            else
            {
                Process_impl(ref obj);
                if (obj != null)
                {
                    if (next_op is IOrderingGrain)
                        (obj as Tuple).seq_token = current_seq_num++;
                    if (next_op != null)
                        next_op.OnNextAsync(obj);
                }
                current_idx++;
                ProcessStashed();
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
