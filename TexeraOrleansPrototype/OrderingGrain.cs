using Orleans;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace TexeraOrleansPrototype
{
    public class OrderingGrain: Grain
    {
        private Dictionary<int, object> stashed = new Dictionary<int, object>();
        private int current_idx = 0;

        public Task OrderingProcess(object obj,int seq_token)
        {
            if (seq_token != current_idx)
                stashed.Add(seq_token, obj);
            else
            {
                Process(obj,seq_token);
                current_idx++;
                ProcessStashed();
            }
            return Task.CompletedTask;
        }

        public virtual Task Process(object row,int seq_token=-2)
        {
            Console.WriteLine("OrderingGrain Process: " + row);
            return Task.CompletedTask;
        }

        private void ProcessStashed()
        {
            if (stashed.ContainsKey(current_idx))
            {
                Process(stashed[current_idx], current_idx);
                stashed.Remove(current_idx);
                current_idx++;
                ProcessStashed();
            }
        }

    }
}
