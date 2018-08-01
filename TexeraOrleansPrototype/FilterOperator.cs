using Orleans;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace TexeraOrleansPrototype
{
    public class FilterOperator : Grain, IFilterOperator
    {
        public bool pause = false;
        public List<Tuple> pausedRows = new List<Tuple>();
        public Task SubmitTuples(Tuple row) {
            if(pause)
            {
                pausedRows.Add(row);
                return Task.CompletedTask;
            }

            IKeywordSearchOperator nextOperator = base.GrainFactory.GetGrain<IKeywordSearchOperator>(this.GetPrimaryKeyLong());
            Console.WriteLine("Filter operator received the tuple with id " + row.id);

            if(row.id == -1 || row.unit_cost > 50)
            {
                Task x = nextOperator.SubmitTuples(row);
                // await x;
                return x;
                // return;
            }
            else
            {
                return Task.CompletedTask;
            }
        }

        public Task PauseOperator()
        {
            pause = true;
            IKeywordSearchOperator nextOperator = base.GrainFactory.GetGrain<IKeywordSearchOperator>(this.GetPrimaryKeyLong());
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

            IKeywordSearchOperator nextOperator = base.GrainFactory.GetGrain<IKeywordSearchOperator>(this.GetPrimaryKeyLong());
            nextOperator.ResumeOperator();
            return Task.CompletedTask;
        }

        public Task QuitOperator()
        {
            IKeywordSearchOperator nextOperator = base.GrainFactory.GetGrain<IKeywordSearchOperator>(this.GetPrimaryKeyLong());
            nextOperator.QuitOperator();
            return Task.CompletedTask;
        }

    }
}