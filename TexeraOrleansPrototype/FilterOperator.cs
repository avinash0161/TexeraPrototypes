using Orleans;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.IO;

namespace TexeraOrleansPrototype
{
    public class FilterOperator : OrderingGrain, IFilterOperator
    {
        public IKeywordSearchOperator nextOperator;
        public override Task OnActivateAsync()
        {
            nextOperator = base.GrainFactory.GetGrain<IKeywordSearchOperator>(this.GetPrimaryKeyLong());
            return base.OnActivateAsync();
        }
        public override Task OnDeactivateAsync()
        {
            return base.OnDeactivateAsync();
        }

        public override Task Process(object row, int seq_token = -2)
        {
            Console.WriteLine("Filter "+ this.GetPrimaryKeyLong()+" Process:" + (row as Tuple).id);
            if ((row as Tuple).id == -1)
                Console.WriteLine("Filter " + this.GetPrimaryKeyLong() + " done");
            if (seq_token != -2)
                nextOperator.OrderingProcess(row, seq_token);
            else
                nextOperator.Process(row);
            return Task.CompletedTask;
        }
    }
}