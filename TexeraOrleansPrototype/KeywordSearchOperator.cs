#define PRINT_MESSAGE_ON
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
    public class OrderedKeywordSearchOperator : OrderingGrain, IOrderedKeywordSearchOperator
    {
        bool finished=false;
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
#if PRINT_MESSAGE_ON
            Console.WriteLine("Ordered KeywordSearch Process: [" + (row as Tuple).seq_token + "] " + (row as Tuple).id);
#endif
#if PRINT_DROPPED_ON
            if (finished)
            Console.WriteLine("Ordered KeywordSearch Process: [" + (row as Tuple).seq_token + "] " + (row as Tuple).id);
#endif
            bool cond = Program.conditions_on ? (row as Tuple).region.Contains("Asia") : true;
            if ((row as Tuple).id == -1)
            {
                Console.WriteLine("Ordered KeywordSearch done");
                finished = true;
            }
            else if (cond)
                (next_op as IOrderedCountOperator).SetAggregatorLevel(true);
            else
                row = null;
            return Task.CompletedTask;
        }
    }

    public class KeywordSearchOperator : NormalGrain, IKeywordSearchOperator
    {
        bool finished = false;
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
#if PRINT_MESSAGE_ON
            Console.WriteLine("Unordered KeywordSearch Process: [" + (row as Tuple).seq_token + "] " + (row as Tuple).id);
#endif
#if PRINT_DROPPED_ON
            if (finished)
            Console.WriteLine("Unordered KeywordSearch Process: [" + (row as Tuple).seq_token + "] " + (row as Tuple).id);
#endif
            bool cond = Program.conditions_on ? (row as Tuple).region.Contains("Asia") : true;
            if ((row as Tuple).id == -1)
            {
                Console.WriteLine("Unordered KeywordSearch done");
                finished = true;
            }
            else if (cond)
                (next_op as ICountOperator).SetAggregatorLevel(true);
            else
                row = null;
            return Task.CompletedTask;
        }
    }
}