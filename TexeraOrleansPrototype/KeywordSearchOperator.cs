using Orleans;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.IO;

namespace TexeraOrleansPrototype
{
    public class OrderedKeywordSearchOperator : OrderingGrain, IOrderedKeywordSearchOperator
    {
        private Guid guid = Guid.NewGuid();
        public override Task OnActivateAsync()
        {
            next_op = this.GrainFactory.GetGrain<IOrderedCountOperator>(this.GetPrimaryKeyLong());
            return base.OnActivateAsync();
        }

        public Task<Guid> GetStreamGuid()
        {
            return Task.FromResult(guid);
        }

        public override Task Process_impl(ref object row)
        {
            if ((row as Tuple).id == -1)
                Console.WriteLine("Ordered KeywordSearch done");
            if (true)
            {
                Console.WriteLine("Ordered KeywordSearch processing: "+(row as Tuple).id);
                (next_op as IOrderedCountOperator).SetAggregatorLevel(true);
            }
            return Task.CompletedTask;
        }
    }

    public class KeywordSearchOperator : NormalGrain, IKeywordSearchOperator
    {
        private Guid guid = Guid.NewGuid();
        public override Task OnActivateAsync()
        {
            next_op = this.GrainFactory.GetGrain<ICountOperator>(this.GetPrimaryKeyLong());
            return base.OnActivateAsync();
        }

        public Task<Guid> GetStreamGuid()
        {
            return Task.FromResult(guid);
        }

        public override Task Process_impl(ref object row)
        {
            if ((row as Tuple).id == -1)
                Console.WriteLine("Unordered KeywordSearch done");
            if (true)
            {
                Console.WriteLine("Unordered KeywordSearch processing: " + (row as Tuple).id);
                (next_op as ICountOperator).SetAggregatorLevel(true);
            }
            return Task.CompletedTask;
        }
    }
}