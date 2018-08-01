using Orleans;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace TexeraOrleansPrototype
{
    public class CountOperator : Grain, ICountOperator
    {
        private Guid guid = Guid.NewGuid();
        public bool pause = false;
        public List<Tuple> pausedRows = new List<Tuple>();
        public List<int> pausedIntermediateAgg = new List<int>();
        public bool isIntermediate = false;
        public int count = 0;
        public int intermediateAggregatorsResponded = 0;

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
            if(pause)
            {
                pausedIntermediateAgg.Add(aggregation);
                return Task.CompletedTask;
            }

            count += aggregation;
            intermediateAggregatorsResponded++;

            if(intermediateAggregatorsResponded == 10)
            {
                var streamProvider = GetStreamProvider("SMSProvider");
                var stream = streamProvider.GetStream<int>(guid, "Random");
                stream.OnNextAsync(count); 
            }
            return Task.CompletedTask;
        }

        public Task SubmitTuples(Tuple row) {
            if(pause)
            {
                pausedRows.Add(row);
                return Task.CompletedTask;
            }
            Console.WriteLine("Count operator received the tuple with id " + row.id);
            if (row.id==-1)
            {
                ICountOperator finalAggregator = this.GrainFactory.GetGrain<ICountOperator>(1);
                finalAggregator.SetAggregatorLevel(false);
                finalAggregator.SubmitIntermediateAgg(count);
            }
            else
                count++;

            return Task.CompletedTask;
        }

        public Task PauseOperator()
        {
            pause = true;
            return Task.CompletedTask;
        }

        public Task ResumeOperator()
        {
            pause = false;
            
            if(pausedRows.Count > 0)
            {
                foreach(Tuple row in pausedRows)
                {
                    SubmitTuples(row);
                }

                pausedRows.Clear();
            }

            if(pausedIntermediateAgg.Count > 0)
            {
                foreach(int agg in pausedIntermediateAgg)
                {
                    SubmitIntermediateAgg(agg);
                }

                pausedIntermediateAgg.Clear();
            }

            return Task.CompletedTask;
        }

        public Task QuitOperator()
        {
            if (isIntermediate)
            {
                ICountOperator finalAggregator = this.GrainFactory.GetGrain<ICountOperator>(1);
                finalAggregator.SetAggregatorLevel(false);
                finalAggregator.SubmitIntermediateAgg(count);
            }
            return Task.CompletedTask;
        }
    }
}