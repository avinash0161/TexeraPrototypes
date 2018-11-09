using Orleans;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Orleans.Concurrency;

namespace TexeraOrleansPrototype
{
    public class OrderingGrain: Grain
    {
        private Dictionary<ulong, Tuple> stashed = new Dictionary<ulong, Tuple>();
        private ulong current_idx = 0;
        private ulong current_seq_num = 0;
        private ulong stashed_count = 0;
        public INormalGrain next_op = null;

        public Task TrivialCall()
        {
            for(int i=0; i< 100000; i++)
            {
                int a = 1;
            }

            return Task.CompletedTask;
        }
        
        public Task Process(Immutable<Tuple> obj)
        {
            var seq_token = obj.Value.seq_token;
            Tuple tuple=obj.Value;
            if(seq_token < current_idx)
            {
                // de-dup messages
                return Task.CompletedTask;
            }
            if (seq_token != current_idx)
            {
                stashed_count++;
                stashed.Add(seq_token, tuple);
            }
            else
            {
                Process_impl(ref tuple);
                if (tuple != null)
                {
                    if(next_op != null)
                    {
                        if (next_op is IOrderingGrain)
                            tuple.seq_token = current_seq_num++;
                        if (next_op != null)
                            next_op.Process(new Immutable<Tuple>(tuple));
                    }
                    
                }
                current_idx++;
                ProcessStashed();
            }
            return Task.CompletedTask;
        }

        public virtual Task Process_impl(ref Tuple row)
        {
            Console.WriteLine("OrderingGrain Process: " + row);
            return Task.CompletedTask;
        }

        public virtual string GetName()
        {
            return "OrderingGrain " + (this.GetPrimaryKeyLong() - 1).ToString();
        }

        public Task PrintStashCount()
        {
            Console.WriteLine(GetName() + " stashed " + stashed_count.ToString() + " messages");
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
                            obj.seq_token = current_seq_num++;
                        if (next_op != null)
                            next_op.Process(new Immutable<Tuple>(obj));
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
