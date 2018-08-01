using Orleans;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace TexeraOrleansPrototype
{
    public class KeywordSearchOperator : Grain, IKeywordSearchOperator
    {
        private Guid guid = Guid.NewGuid();
        public bool pause = false; 
        public List<Tuple> pausedRows = new List<Tuple>();
        public Task<Guid> GetStreamGuid()
        {
            return Task.FromResult(guid);
        }
        public Task SubmitTuples(Tuple row) {
            // Thread.Sleep(3000);
            if(pause)
            {
                pausedRows.Add(row);
                return Task.CompletedTask;
            }
            
            Console.WriteLine("Keyword operator received the tuple with id " + row.id);

            if(row.region.Contains("China"))
            {
                ICountOperator nextOperator = this.GrainFactory.GetGrain<ICountOperator>(500);
                nextOperator.SetAggregatorLevel(true);
                nextOperator.SubmitTuples(row);
            }
            
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

            return Task.CompletedTask;
        }
    }
}