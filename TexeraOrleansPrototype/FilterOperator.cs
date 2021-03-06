using Orleans;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.IO;

namespace TexeraOrleansPrototype
{
    public class FilterOperator : Grain, IFilterOperator
    {
        public bool pause = false;
        public List<Tuple> pausedRows = new List<Tuple>();
        public FileStream fs;
        public StreamWriter sw;
        public IKeywordSearchOperator nextOperator;

        public override Task OnActivateAsync()
        {
            string path = "Filter_" + this.GetPrimaryKeyLong().ToString();
            fs = new FileStream(path, FileMode.Create);
            sw = new StreamWriter(fs);
            nextOperator = base.GrainFactory.GetGrain<IKeywordSearchOperator>(this.GetPrimaryKeyLong());
            return base.OnActivateAsync();
        }
        public override Task OnDeactivateAsync()
        {
            sw.Flush();
            fs.Close();
            return base.OnDeactivateAsync();
        }
        public async Task SubmitTuples(Tuple row) {
            if(pause)
            {
                pausedRows.Add(row);
                //return Task.CompletedTask;
            }

            // IKeywordSearchOperator nextOperator = base.GrainFactory.GetGrain<IKeywordSearchOperator>(this.GetPrimaryKeyLong());
          
            // if (row.id == -1 || row.unit_cost > 50)
            // if (row.id != -1)
            //     sw.WriteLine(row.id);
            if(true)
            {
                await nextOperator.SubmitTuples(row);
                // await x;
                //return x;
                // return;
            }
            //Console.WriteLine("Filter operator received the tuple with id " + row.id);
        }

        public async Task PauseOperator()
        {
            pause = true;
            IKeywordSearchOperator nextOperator = base.GrainFactory.GetGrain<IKeywordSearchOperator>(this.GetPrimaryKeyLong());
            await nextOperator.PauseOperator();
        }

        public async Task ResumeOperator()
        {
            pause = false;
            
            if(pausedRows.Count > 0)
            {
                foreach(Tuple row in pausedRows)
                {
                    await SubmitTuples(row);
                }

                pausedRows.Clear();
            }

            IKeywordSearchOperator nextOperator = base.GrainFactory.GetGrain<IKeywordSearchOperator>(this.GetPrimaryKeyLong());
            await nextOperator.ResumeOperator();
        }

        public async Task QuitOperator()
        {
            sw.Flush();
            fs.Close();
            IKeywordSearchOperator nextOperator = base.GrainFactory.GetGrain<IKeywordSearchOperator>(this.GetPrimaryKeyLong());
            await nextOperator.QuitOperator();
        }

    }
}