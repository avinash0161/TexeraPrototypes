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
        public FileStream fs;
        public StreamWriter sw;

        public IFilterOperator nextOperator;
        System.IO.StreamReader file;

        public Task WakeUp()
        {
            return Task.CompletedTask;
        }


        public override Task OnActivateAsync()
        {
            string path = "Scan_" + this.GetPrimaryKeyLong().ToString();
            fs = new FileStream(path, FileMode.Create);
            sw = new StreamWriter(fs);
            nextOperator = base.GrainFactory.GetGrain<IFilterOperator>(this.GetPrimaryKeyLong());
            string p2 = @"d:\small_input_" + (this.GetPrimaryKeyLong() - 1) + ".csv";
            Console.WriteLine(p2);
            file = new System.IO.StreamReader(p2);
            return base.OnActivateAsync();
        }
        public override Task OnDeactivateAsync()
        {
            sw.Flush();
            fs.Close();
            return base.OnDeactivateAsync();
        }

        public async Task SubmitTuples(List<Tuple> rows) 
        {
            if(pause)
            {
                pausedRows.AddRange(rows);
                //return Task.CompletedTask;
            }

            string line;
            int count = 0;
            // IFilterOperator nextOperator = base.GrainFactory.GetGrain<IFilterOperator>(this.GetPrimaryKeyLong());
            //Console.WriteLine("Scan operator received the tuples");
            while ((line = file.ReadLine()) != null)
            {
                nextOperator.SubmitTuples(new Tuple(count, line.Split(",")));
                count++;
            }
            nextOperator.SubmitTuples(new Tuple(-1, null));

            Console.WriteLine("Scan "+ this.GetPrimaryKeyLong().ToString() + " done");

            //return Task.CompletedTask;
            // Task x = nextOperator.SubmitTuples(row);
            // await x;
            // return x;
            // return;
        }

        public async Task PauseOperator()
        {
            pause = true;
            IFilterOperator nextOperator = base.GrainFactory.GetGrain<IFilterOperator>(this.GetPrimaryKeyLong());
            await nextOperator.PauseOperator();
            //return Task.CompletedTask;
        }

        public async Task ResumeOperator()
        {
            pause = false;
            
            if(pausedRows.Count > 0)
            {
                await SubmitTuples(pausedRows);
                pausedRows.Clear();
            }
            IFilterOperator nextOperator = base.GrainFactory.GetGrain<IFilterOperator>(this.GetPrimaryKeyLong());
            await nextOperator.ResumeOperator();
            //return Task.CompletedTask;
        }
        public async Task QuitOperator()
        {
            sw.Flush();
            fs.Close();
            IFilterOperator nextOperator = base.GrainFactory.GetGrain<IFilterOperator>(this.GetPrimaryKeyLong());
            await nextOperator.QuitOperator();
            //return Task.CompletedTask;
        }
    }
}