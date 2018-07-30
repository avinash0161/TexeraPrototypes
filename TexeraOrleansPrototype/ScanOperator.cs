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
        public Task SubmitTuples(List<Tuple> rows) {

            IFilterOperator nextOperator = base.GrainFactory.GetGrain<IFilterOperator>(500);
            Console.WriteLine("Scan operator received the temperature");

            foreach(Tuple row in rows)
            {
                nextOperator.SubmitTuples(row);
            }

            return Task.CompletedTask;
            // Task x = nextOperator.SubmitTuples(row);
            // await x;
            // return x;
            // return;
        }
    }
}