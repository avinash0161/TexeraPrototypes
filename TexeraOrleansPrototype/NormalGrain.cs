using Orleans;
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
        public Task Process(object obj)
        {
            Process_impl(ref obj);
            if (obj != null)
            {
                if (next_op is IOrderingGrain)
                    (obj as Tuple).seq_token = current_seq_num++;
                if (next_op != null)
                    next_op.Process(obj);
            }
            return Task.CompletedTask;
        }

        public virtual Task Process_impl(ref object row)
        {
            Console.WriteLine("OrderingGrain Process: " + row);
            return Task.CompletedTask;
        }
    }
}
