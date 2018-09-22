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

        public INormalGrain nextOperator;
        System.IO.StreamReader file;

        public override Task OnActivateAsync()
        {
            nextOperator = base.GrainFactory.GetGrain<IFilterOperator>(this.GetPrimaryKeyLong());
            string p2 = @"d:\median_input_" + (this.GetPrimaryKeyLong() - 1) + ".csv";
            //string p2 = @"d:\median_input.csv";
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
            ulong count = 0;
            while ((line = file.ReadLine()) != null)
            {
                nextOperator.Process(new Tuple(count,(int)count, line.Split(",")));
                count++;
            }
            nextOperator.Process(new Tuple(count ,- 1, null));

            Console.WriteLine("Scan "+ this.GetPrimaryKeyLong().ToString() + " done");
        }

       
    }
}