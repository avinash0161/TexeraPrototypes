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
            ICountOperator nextOperator = this.GrainFactory.GetGrain<ICountOperator>(this.GetPrimaryKeyLong());
            nextOperator.SetAggregatorLevel(true);
            if (row.region.Contains("Asia"))
            {
                nextOperator.SubmitTuples(row);
            }
            
            return Task.CompletedTask;     
        }

        public Task PauseOperator()
        {
            pause = true;
            ICountOperator nextOperator = this.GrainFactory.GetGrain<ICountOperator>(this.GetPrimaryKeyLong());
            nextOperator.PauseOperator();
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
            ICountOperator nextOperator = this.GrainFactory.GetGrain<ICountOperator>(this.GetPrimaryKeyLong());
            nextOperator.ResumeOperator();

            return Task.CompletedTask;
        }
        public Task QuitOperator()
        {
            ICountOperator nextOperator = this.GrainFactory.GetGrain<ICountOperator>(this.GetPrimaryKeyLong());
            nextOperator.QuitOperator();
            return Task.CompletedTask;
        }
    }
}