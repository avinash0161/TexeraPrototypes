using Orleans;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.IO;

namespace TexeraOrleansPrototype
{
    public class ScanOperator : Grain, IScanOperator
    {
        public bool pause = false;
        public List<Tuple> pausedRows = new List<Tuple>();

        public IFilterOperator nextOperator;
        System.IO.StreamReader file;

        public override Task OnActivateAsync()
        {
            nextOperator = base.GrainFactory.GetGrain<IFilterOperator>(this.GetPrimaryKeyLong());
            string p2 = @"/home/shengqun/data/small_input_" + (this.GetPrimaryKeyLong() - 1) + ".csv";
            file = new System.IO.StreamReader(p2);
            return base.OnActivateAsync();
        }
        public override Task OnDeactivateAsync()
        {
            return base.OnDeactivateAsync();
        }

        public async Task SubmitTuples() 
        {
            string line;
            int count = 0;
            while ((line = file.ReadLine()) != null)
            {
                nextOperator.OrderingProcess(new Tuple(count, line.Split(",")),count);
                count++;
            }
            nextOperator.OrderingProcess(new Tuple(-1, null),count);

            Console.WriteLine("Scan "+ this.GetPrimaryKeyLong().ToString() + " done");
        }

       
    }
}