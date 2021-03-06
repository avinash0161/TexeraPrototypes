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

        public IJoinOperator nextOperator;

        public override Task OnActivateAsync()
        {
            string path = "Scan_" + this.GetPrimaryKeyLong().ToString();
            fs = new FileStream(path, FileMode.Create);
            sw = new StreamWriter(fs);
            nextOperator = base.GrainFactory.GetGrain<IJoinOperator>(1);
            return base.OnActivateAsync();
        }
        public override Task OnDeactivateAsync()
        {
            sw.Flush();
            fs.Close();
            return base.OnDeactivateAsync();
        }

        public async Task SubmitTuples(List<Tuple> rows,bool isLeft) 
        {
            if(pause)
            {
                pausedRows.AddRange(rows);
                //return Task.CompletedTask;
            }


            // IFilterOperator nextOperator = base.GrainFactory.GetGrain<IFilterOperator>(this.GetPrimaryKeyLong());
            //Console.WriteLine("Scan operator received the tuples");

            foreach(Tuple row in rows)
            {
                // if (row.id != -1)
                //     sw.WriteLine(row.id);
                Console.WriteLine("sending row with id " + row.id);
                await nextOperator.SubmitTuples(row,isLeft);
                // Thread.Sleep(2000);
            }

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
                await SubmitTuples(pausedRows,true);
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