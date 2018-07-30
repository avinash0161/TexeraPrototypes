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
        public Task SubmitTuples(Tuple row) {

            IFilterOperator nextOperator = base.GrainFactory.GetGrain<IFilterOperator>(500);
            Console.WriteLine("Scan operator received the temperature");
            Task x = nextOperator.SubmitTuples(row);
            // await x;
            return x;
            // return;
        }
    }
}