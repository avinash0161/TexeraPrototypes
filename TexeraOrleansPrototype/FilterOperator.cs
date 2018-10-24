// #define PRINT_MESSAGE_ON
//#define PRINT_DROPPED_ON


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
        bool finished = false;
        public override Task OnActivateAsync()
        {
            next_op = base.GrainFactory.GetGrain<IOrderedKeywordSearchOperator>(this.GetPrimaryKeyLong());
            return base.OnActivateAsync();
        }

        public override Task Process_impl(ref Tuple row)
        {
#if PRINT_MESSAGE_ON
            Console.WriteLine("Ordered Filter Process: [" + (row as Tuple).seq_token + "] " + (row as Tuple).id);
#endif
#if PRINT_DROPPED_ON
            if (finished)
            Console.WriteLine("Ordered Filter Process: [" + (row as Tuple).seq_token + "] " + (row as Tuple).id);
#endif
            bool cond = Program.conditions_on ? (row as Tuple).unit_cost > 50 : true;
            if ((row as Tuple).id == -1)
            {
                Console.WriteLine("Ordered Filter " + (this.GetPrimaryKeyLong() - 1).ToString() + " done ");
                finished = true;
            }
            else if (!cond)
                row = null;
            return Task.CompletedTask;
        }
    }

    public class FilterOperator : NormalGrain, IFilterOperator
    {
        bool finished = false;
        public override Task OnActivateAsync()
        {
            next_op = base.GrainFactory.GetGrain<IKeywordSearchOperator>(this.GetPrimaryKeyLong());
            return base.OnActivateAsync();
        }

        public override Task Process_impl(ref Tuple row)
        {
#if PRINT_MESSAGE_ON
            Console.WriteLine("Unordered Filter Process: [" + (row as Tuple).seq_token + "] " + (row as Tuple).id);
#endif
#if PRINT_DROPPED_ON
            if (finished)
            Console.WriteLine("Unordered Filter Process: [" + (row as Tuple).seq_token + "] " + (row as Tuple).id);
#endif
            bool cond = Program.conditions_on ? (row as Tuple).unit_cost > 50 : true;
            if ((row as Tuple).id == -1)
            {
                Console.WriteLine("Unordered Filter done");
                finished = true;
            }
            else if (!cond)
                row = null;
            return Task.CompletedTask;
        }
    }

}