using Orleans;
using Orleans.Concurrency;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace TexeraOrleansPrototype
{
    public class NormalGrain : Grain
    {
        private ulong current_seq_num = 0;
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
            Tuple tuple=obj.Value;

            Process_impl(ref tuple);
            if (tuple != null)
            {
                if (next_op is IOrderingGrain)
                    tuple.seq_token = current_seq_num++;
                if (next_op != null)
                    next_op.Process(new Immutable<Tuple>(tuple));
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
            return "NormalGrain " + (this.GetPrimaryKeyLong() - 1).ToString();
        }


    }
}
