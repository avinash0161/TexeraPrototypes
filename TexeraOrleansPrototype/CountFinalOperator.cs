using Orleans;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.IO;
namespace TexeraOrleansPrototype
{

    public class CountFinalOperator : NormalGrain, ICountFinalOperator
    {
        public int count = 0;
        public int intermediateAggregatorsResponded = 0;

        public override Task OnActivateAsync()
        {
            var streamProvider = GetStreamProvider("SMSProvider");
            current_op = streamProvider.GetStream<object>(Guid.Empty, "CountFinal");
            return base.OnActivateAsync();
        }


        public override Task Process_impl(ref object row)
        {
            count += (int)row;
            intermediateAggregatorsResponded++;

            if (intermediateAggregatorsResponded == TexeraConfig.num_scan)
            {
                row = count;
            }
            else
                row = null;
            return Task.CompletedTask;
        }
    }

}