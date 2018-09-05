using Orleans;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.IO;

namespace TexeraOrleansPrototype
{
    public class KeywordSearchOperator : OrderingGrain, IKeywordSearchOperator
    {
        private Guid guid = Guid.NewGuid();
        ICountOperator nextOperator;
        public override Task OnActivateAsync()
        {
            nextOperator = this.GrainFactory.GetGrain<ICountOperator>(this.GetPrimaryKeyLong());
            return base.OnActivateAsync();
        }
        public override Task OnDeactivateAsync()
        {
            return base.OnDeactivateAsync();
        }

        public Task<Guid> GetStreamGuid()
        {
            return Task.FromResult(guid);
        }


        public override Task Process(object row, int seq_token = -2)
        {
            if ((row as Tuple).id == -1)
                Console.WriteLine("KeywordSearch " + this.GetPrimaryKeyLong() + " done");
            if (true)
            {
                Console.WriteLine("KeywordSearch " + this.GetPrimaryKeyLong() + " processing: " + (row as Tuple).id);
                nextOperator.SetAggregatorLevel(true);
                if (seq_token != -2)
                    nextOperator.OrderingProcess(row, seq_token);
                else
                    nextOperator.Process(row);
            }
            return Task.CompletedTask;
        }

    }
}