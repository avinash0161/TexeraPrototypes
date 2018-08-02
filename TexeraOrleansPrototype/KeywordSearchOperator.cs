using Orleans;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.IO;

namespace TexeraOrleansPrototype
{
    public class KeywordSearchOperator : Grain, IKeywordSearchOperator
    {
        private Guid guid = Guid.NewGuid();
        public bool pause = false;
        public List<Tuple> pausedRows = new List<Tuple>();

        public FileStream fs;
        public StreamWriter sw;

        public override Task OnActivateAsync()
        {
            string path = "KeywordSearch_" + this.GetPrimaryKeyLong().ToString();
            fs = new FileStream(path, FileMode.Create);
            sw = new StreamWriter(fs);
            return base.OnActivateAsync();
        }
        public override Task OnDeactivateAsync()
        {
            sw.Flush();
            fs.Close();
            return base.OnDeactivateAsync();
        }

        public Task<Guid> GetStreamGuid()
        {
            return Task.FromResult(guid);
        }
        public async Task SubmitTuples(Tuple row) {
            // Thread.Sleep(3000);
            if(pause)
            {
                pausedRows.Add(row);
                //return Task.CompletedTask;
            }

            //Console.WriteLine("Keyword operator received the tuple with id " + row.id);
            // if (row.id==-1 || row.region.Contains("Asia"))
            if (row.id != -1)
                sw.WriteLine(row.id);
            if (true)
            {
                ICountOperator nextOperator = this.GrainFactory.GetGrain<ICountOperator>(this.GetPrimaryKeyLong());
                nextOperator.SetAggregatorLevel(true);
                nextOperator.SubmitTuples(row);
            }
            
            //return Task.CompletedTask;     
        }

        public async Task PauseOperator()
        {
            pause = true;
            ICountOperator nextOperator = this.GrainFactory.GetGrain<ICountOperator>(this.GetPrimaryKeyLong());
            await nextOperator.PauseOperator();
            //return Task.CompletedTask;
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
            ICountOperator nextOperator = this.GrainFactory.GetGrain<ICountOperator>(this.GetPrimaryKeyLong());
            await nextOperator.ResumeOperator();

            //return Task.CompletedTask;
        }
        public async Task QuitOperator()
        {
            sw.Flush();
            fs.Close();
            ICountOperator nextOperator = this.GrainFactory.GetGrain<ICountOperator>(this.GetPrimaryKeyLong());
            await nextOperator.QuitOperator();
            //return Task.CompletedTask;
        }
    }
}