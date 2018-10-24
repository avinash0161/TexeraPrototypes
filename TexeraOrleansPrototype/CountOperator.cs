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
        // public Task SetAggregatorLevel(bool isIntermediate)
        // {
        //     this.isIntermediate = isIntermediate;
        //     return Task.CompletedTask;
        // }

        // public Task<Guid> GetStreamGuid()
        // {
        //     return Task.FromResult(guid);
        // }

        // public Task SubmitIntermediateAgg(int aggregation)
        // {
        //     count += aggregation;
        //     intermediateAggregatorsResponded++;

        //     if (intermediateAggregatorsResponded == Program.num_scan)
        //     {
        //         var streamProvider = GetStreamProvider("SMSProvider");
        //         var stream = streamProvider.GetStream<int>(guid, "Random");
        //         stream.OnNextAsync(count);
        //     }
        //     return Task.CompletedTask;
        // }

        public override Task Process_impl(ref Tuple row)
        {
            if ((row as Tuple).id == -1)
            {
                IOrderedCountFinalOperator finalAggregator = this.GrainFactory.GetGrain<IOrderedCountFinalOperator>(1);
                // finalAggregator.SetAggregatorLevel(false);
                finalAggregator.SubmitIntermediateAgg(count);
            }
            else
            {
                //Console.WriteLine("Ordered Count processing: [" + (row as Tuple).seq_token + "] " + (row as Tuple).id);
                count++;
            }
            return Task.CompletedTask;
        }
    }



    public class CountOperator : NormalGrain, ICountOperator
    {
        private Guid guid = Guid.NewGuid();
        // public bool isIntermediate = false;
        public int count = 0;
        public int intermediateAggregatorsResponded = 0;
        // public Task SetAggregatorLevel(bool isIntermediate)
        // {
        //     this.isIntermediate = isIntermediate;
        //     return Task.CompletedTask;
        // }

        // public Task<Guid> GetStreamGuid()
        // {
        //     return Task.FromResult(guid);
        // }

        // public Task SubmitIntermediateAgg(int aggregation)
        // {
        //     count += aggregation;
        //     intermediateAggregatorsResponded++;

        //     if (intermediateAggregatorsResponded == Program.num_scan)
        //     {
        //         var streamProvider = GetStreamProvider("SMSProvider");
        //         var stream = streamProvider.GetStream<int>(guid, "Random");
        //         stream.OnNextAsync(count);
        //     }
        //     return Task.CompletedTask;
        // }

        public override Task Process_impl(ref Tuple row)
        {
            if ((row as Tuple).id == -1)
            {
                ICountFinalOperator finalAggregator = this.GrainFactory.GetGrain<ICountFinalOperator>(1);
                // finalAggregator.SetAggregatorLevel(false);
                finalAggregator.SubmitIntermediateAgg(count);
            }
            else
            {
                //Console.WriteLine("Unordered Count processing: [" + (row as Tuple).seq_token + "] " + (row as Tuple).id);
                count++;
            }
            return Task.CompletedTask;
        }
    }

}