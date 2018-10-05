using Orleans;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace TexeraOrleansPrototype
{
    public class OrderingGrain: Grain
    {
        private Dictionary<ulong, object> stashed = new Dictionary<ulong, object>();
        private ulong current_idx = 0;
        private ulong current_seq_num = 0;
        public INormalGrain next_op = null;
        public Task Process(object obj)
        {
            var seq_token = (obj as Tuple).seq_token;
            if(seq_token < current_idx)
            {
                // de-dup messages
                return Task.CompletedTask;
            }
            if (seq_token != current_idx)
            {
                stashed.Add(seq_token, obj);
                Console.WriteLine("Stashed " + seq_token + " !");
            }
            else
            {
                Process_impl(ref obj);
                if (obj != null)
                {
                    if (next_op is IOrderingGrain)
                        (obj as Tuple).seq_token = current_seq_num++;
                    if (next_op != null)
                        next_op.Process(obj);
                }
                current_idx++;
                ProcessStashed();
            }
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
                            next_op.Process(obj);
                    }
                    stashed.Remove(current_idx);
                    current_idx++;
                }
                else
                    break;
            }
        }

    }
}
