using Orleans;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.IO;
namespace TexeraOrleansPrototype
{

    public class OrderedCountOperator : OrderingGrain, IOrderedCountOperator
    {
        private Guid guid = Guid.NewGuid();
        public bool isIntermediate = false;
        public int count = 0;
        public int intermediateAggregatorsResponded = 0;

        public override Task Process_impl(ref Tuple row)
        {
            if ((row as Tuple).id == -1)
            {
                IOrderedCountFinalOperator finalAggregator = this.GrainFactory.GetGrain<IOrderedCountFinalOperator>(1);
                finalAggregator.SubmitIntermediateAgg(count);
            }
            else
            {
                count++;
            }
            return Task.CompletedTask;
        }
        public override string GetName()
        {
            return "Ordered Count " + (this.GetPrimaryKeyLong() - 1).ToString();
        }
    }



    public class CountOperator : NormalGrain, ICountOperator
    {
        private Guid guid = Guid.NewGuid();
        // public bool isIntermediate = false;
        public int count = 0;
        public int intermediateAggregatorsResponded = 0;
       
        public override Task Process_impl(ref Tuple row)
        {
            if ((row as Tuple).id == -1)
            {
                ICountFinalOperator finalAggregator = this.GrainFactory.GetGrain<ICountFinalOperator>(1);
                finalAggregator.SubmitIntermediateAgg(count);
            }
            else
            {
                count++;
            }
            return Task.CompletedTask;
        }
        public override string GetName()
        {
            return "Unordered Count " + (this.GetPrimaryKeyLong() - 1).ToString();
        }
    }

}