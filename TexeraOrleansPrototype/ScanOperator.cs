using Orleans;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace TexeraOrleansPrototype
{
    public class ScanOperator : Grain, IScanOperator
    {
        public bool pause = false;
        public List<Tuple> pausedRows = new List<Tuple>();
        public Task SubmitTuples(List<Tuple> rows) 
        {
            if(pause)
            {
                pausedRows.AddRange(rows);
                return Task.CompletedTask;
            }


            IFilterOperator nextOperator = base.GrainFactory.GetGrain<IFilterOperator>(this.GetPrimaryKeyLong());
            Console.WriteLine("Scan operator received the tuples");

            foreach(Tuple row in rows)
            {
                Console.WriteLine("Scan operator sending next tuple with id "+ row.id);
                nextOperator.SubmitTuples(row);
                // Thread.Sleep(2000);
            }

            return Task.CompletedTask;
            // Task x = nextOperator.SubmitTuples(row);
            // await x;
            // return x;
            // return;
        }

        public Task PauseOperator()
        {
            pause = true;
            IFilterOperator nextOperator = base.GrainFactory.GetGrain<IFilterOperator>(this.GetPrimaryKeyLong());
            nextOperator.PauseOperator();
            return Task.CompletedTask;
        }

        public Task ResumeOperator()
        {
            pause = false;
            
            if(pausedRows.Count > 0)
            {
                SubmitTuples(pausedRows);
                pausedRows.Clear();
            }
            IFilterOperator nextOperator = base.GrainFactory.GetGrain<IFilterOperator>(this.GetPrimaryKeyLong());
            nextOperator.ResumeOperator();
            return Task.CompletedTask;
        }
        public Task QuitOperator()
        {
            IFilterOperator nextOperator = base.GrainFactory.GetGrain<IFilterOperator>(this.GetPrimaryKeyLong());
            nextOperator.QuitOperator();
            return Task.CompletedTask;
        }
    }
}