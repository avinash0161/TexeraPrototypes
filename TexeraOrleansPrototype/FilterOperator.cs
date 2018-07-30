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
        public Task SubmitTuples(Tuple row) {

            IKeywordSearchOperator nextOperator = base.GrainFactory.GetGrain<IKeywordSearchOperator>(500);
            Console.WriteLine("Scan operator received the temperature");

            if(row.followers == 34)
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
    }
}