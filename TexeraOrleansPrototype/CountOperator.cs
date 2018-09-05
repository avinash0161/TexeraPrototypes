using Orleans;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.IO;
namespace TexeraOrleansPrototype
{
    public class CountOperator : OrderingGrain, ICountOperator
    {
        private Guid guid = Guid.NewGuid();
        public bool isIntermediate = false;
        public int count = 0;
        public int intermediateAggregatorsResponded = 0;

        public override Task OnActivateAsync()
        {
            return base.OnActivateAsync();
        }
        public override Task OnDeactivateAsync()
        {
            return base.OnDeactivateAsync();
        }

        public Task SetAggregatorLevel(bool isIntermediate)
        {
            this.isIntermediate = isIntermediate;
            return Task.CompletedTask;
        }

        public Task<Guid> GetStreamGuid()
        {
            return Task.FromResult(guid);
        }

        public Task SubmitIntermediateAgg(int aggregation)
        {
            
            count += aggregation;
            intermediateAggregatorsResponded++;

            if(intermediateAggregatorsResponded == Program.num_scan)
            {
                var streamProvider = GetStreamProvider("SMSProvider");
                var stream = streamProvider.GetStream<int>(guid, "Random");
                stream.OnNextAsync(count); 
            }
            return Task.CompletedTask;
        }

        public override Task Process(object row, int seq_token = -2)
        {
            if ((row as Tuple).id == -1)
            {
                ICountOperator finalAggregator = this.GrainFactory.GetGrain<ICountOperator>(0);
                finalAggregator.SetAggregatorLevel(false);
                finalAggregator.SubmitIntermediateAgg(count);
            }
            else
            {
                Console.WriteLine("Count " + this.GetPrimaryKeyLong() + " processing: " + (row as Tuple).id);
                count++;
            }
            return Task.CompletedTask;
        }

      
    }
}