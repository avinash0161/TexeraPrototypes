using Orleans;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.IO;

namespace TexeraOrleansPrototype
{
    public class OrderedFilterOperator : OrderingGrain, IOrderedFilterOperator
    {
        public override Task OnActivateAsync()
        {
            next_op = base.GrainFactory.GetGrain<IOrderedKeywordSearchOperator>(this.GetPrimaryKeyLong());
            return base.OnActivateAsync();
        }

        public override Task Process_impl(ref object row)
        {
            Console.WriteLine("Ordered Filter Process: [" + (row as Tuple).seq_token + "] " + (row as Tuple).id);
            if ((row as Tuple).id == -1)
                Console.WriteLine("Ordered Filter done");
            else if (!((row as Tuple).unit_cost > 50))
                row = null;
            return Task.CompletedTask;
        }
    }

    public class FilterOperator : NormalGrain, IFilterOperator
    {
        public override Task OnActivateAsync()
        {
            next_op = base.GrainFactory.GetGrain<IKeywordSearchOperator>(this.GetPrimaryKeyLong());
            return base.OnActivateAsync();
        }

        public override Task Process_impl(ref object row)
        {
            Console.WriteLine("Unordered Filter Process: [" + (row as Tuple).seq_token + "] " + (row as Tuple).id);
            if ((row as Tuple).id == -1)
                Console.WriteLine("Unordered Filter done");
            else if (!((row as Tuple).unit_cost > 50))
                row = null;
            return Task.CompletedTask;
        }
    }

}