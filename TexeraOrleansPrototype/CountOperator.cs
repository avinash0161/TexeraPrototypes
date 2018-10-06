using Orleans;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.IO;
namespace TexeraOrleansPrototype
{

    public class OrderedCountOperator : OrderingGrain, IOrderedCountOperator
    {
        int count = 0;
        public override Task OnActivateAsync()
        {
            var streamProvider = GetStreamProvider("SMSProvider");
            current_op = streamProvider.GetStream<object>(this.GetPrimaryKey(), "OrderedCount");
            return base.OnActivateAsync();
        }

        public override Task Process_impl(ref object row)
        {
            if ((row as Tuple).id == -1)
            {
                row = count;
            }
            else
            {
                //Console.WriteLine("Ordered Count processing: [" + (row as Tuple).seq_token + "] " + (row as Tuple).id);
                count++;
                row = null;
            }
            return Task.CompletedTask;
        }
    }



    public class CountOperator : NormalGrain, ICountOperator
    {
        int count = 0;

        public override Task OnActivateAsync()
        {
            var streamProvider = GetStreamProvider("SMSProvider");
            current_op = streamProvider.GetStream<object>(this.GetPrimaryKey(), "Count");
            return base.OnActivateAsync();
        }

      
        public override Task Process_impl(ref object row)
        {
            if ((row as Tuple).id == -1)
            {
                row = count;
            }
            else
            {
                //Console.WriteLine("Unordered Count processing: [" + (row as Tuple).seq_token + "] " + (row as Tuple).id);
                count++;
                row = null;
            }
            return Task.CompletedTask;
        }
    }

}