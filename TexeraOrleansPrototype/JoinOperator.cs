using Orleans;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.IO;
namespace TexeraOrleansPrototype
{
    public class JoinOperator : Grain, IJoinOperator
    {
        private Guid guid = Guid.NewGuid();
        public bool pause = false;
        public List<Tuple> pausedRows = new List<Tuple>();
        public Dictionary<string, List<int>> leftTable = new Dictionary<string, List<int>>();
        public Dictionary<string, List<int>> rightTable = new Dictionary<string, List<int>>();
        public int leftCount = 0;
        public int rightCount = 0;

        //public FileStream fs;
        //public StreamWriter sw;

        public override Task OnActivateAsync()
        {
            //string path = "Count_" + this.GetPrimaryKeyLong().ToString();
            //fs = new FileStream(path, FileMode.Create);
            //sw = new StreamWriter(fs);
            return base.OnActivateAsync();
        }
        public override Task OnDeactivateAsync()
        {
            //sw.Flush();
            //fs.Close();
            return base.OnDeactivateAsync();
        }
       
        public Task<Guid> GetStreamGuid()
        {
            return Task.FromResult(guid);
        }


        public async Task SubmitTuples(Tuple row,bool isLeft) {
            if(pause)
            {
                pausedRows.Add(row);
                //return Task.CompletedTask;
            }

            Console.WriteLine("receiving row with id " + row.id);
            if (row.id == -1)
            {
                if (isLeft)
                    leftCount++;
                else
                    rightCount++;
                if(leftCount==10 && rightCount==10)
                {
                    var streamProvider = GetStreamProvider("SMSProvider");
                    var stream = streamProvider.GetStream<int>(guid, "Random");
                    stream.OnNextAsync(0);
                }

            }
            else
            {
                if (isLeft)
                {
                    if (leftTable.ContainsKey(row.region))
                    {
                        leftTable[row.region].Add(row.id);
                    }
                    else
                        leftTable.Add(row.region, new List<int> { row.id });
                    if (rightTable.ContainsKey(row.region))
                    {
                        Console.Write("join " + row.id + " with (");
                        foreach (var i in rightTable[row.region])
                            Console.Write(i + ",");
                        Console.WriteLine(") region: " + row.region);
                    }
                }
                else
                {
                    if (rightTable.ContainsKey(row.region))
                    {
                        rightTable[row.region].Add(row.id);
                    }
                    else
                        rightTable.Add(row.region, new List<int> { row.id });
                    if (leftTable.ContainsKey(row.region))
                    {
                        Console.Write("join " + row.id + " with (");
                        foreach (var i in leftTable[row.region])
                            Console.Write(i + ",");
                        Console.WriteLine(") region: "+row.region);
                    }
                }
            }
            //Console.WriteLine("Count operator received the tuple with id " + row.id);
            //return Task.CompletedTask;
        }

        public async Task PauseOperator()
        {
            pause = true;
            //return Task.CompletedTask;
        }

        public async Task ResumeOperator()
        {
            pause = false;
            
            if(pausedRows.Count > 0)
            {
                foreach(Tuple row in pausedRows)
                {
                    await SubmitTuples(row,true);
                }

                pausedRows.Clear();
            }
            /*
            if(pausedIntermediateAgg.Count > 0)
            {
                foreach(int agg in pausedIntermediateAgg)
                {
                    SubmitIntermediateAgg(agg);
                }

                pausedIntermediateAgg.Clear();
            }
            */
            //return Task.CompletedTask;
        }

        public async Task QuitOperator()
        {
            //sw.Flush();
            //fs.Close();
            //return Task.CompletedTask;
        }
    }
}