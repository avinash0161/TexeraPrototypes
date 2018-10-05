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
        public List<Tuple> Rows = new List<Tuple>();
        public INormalGrain nextOperator;
        System.IO.StreamReader file;

        public override Task OnActivateAsync()
        {
            nextOperator = base.GrainFactory.GetGrain<IOrderedFilterOperator>(this.GetPrimaryKeyLong());
            //string p2 = @"d:\large_input_" + (this.GetPrimaryKeyLong() - 1) + ".csv";
            string p2 = @"d:\median_input.csv";
            file = new System.IO.StreamReader(p2);
            return base.OnActivateAsync();
        }
        public override Task OnDeactivateAsync()
        {
            return base.OnDeactivateAsync();
        }

        public Task SubmitTuples() 
        {
            for (int i = 0; i < Rows.Count; ++i)
                nextOperator.Process(Rows[i]);
            nextOperator.Process(new Tuple((ulong)Rows.Count ,- 1, null));
            Console.WriteLine("Scan " + (this.GetPrimaryKeyLong() - 1).ToString() + " sending done");
            return Task.CompletedTask;
        }


        public Task LoadTuples()
        {
            string line;
            ulong count = 0;
            while ((line = file.ReadLine()) != null)
            {
                Rows.Add(new Tuple(count, (int)count, line.Split(",")));
                count++;
            }
            Console.WriteLine("Scan " + (this.GetPrimaryKeyLong() - 1).ToString() + " loading done");
            return Task.CompletedTask;
        }
       
    }
}