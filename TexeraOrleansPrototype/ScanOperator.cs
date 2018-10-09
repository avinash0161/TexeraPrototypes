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
            if(Program.ordered_on)
                nextOperator = base.GrainFactory.GetGrain<IOrderedFilterOperator>(this.GetPrimaryKeyLong());
            else
                nextOperator = base.GrainFactory.GetGrain<IFilterOperator>(this.GetPrimaryKeyLong());
            string p2;
            if (Program.num_scan == 1)
                p2 = Program.dir + Program.dataset + "_input.csv";
            else
                p2 = Program.dir + Program.dataset + "_input" + "_" + (this.GetPrimaryKeyLong() - 1) + ".csv";
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
            {
		Console.WriteLine("Scan " + (this.GetPrimaryKeyLong() - 1).ToString() + " sending "+i.ToString());
		nextOperator.Process(Rows[i]);
	        }
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