using Orleans;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace TexeraOrleansPrototype
{
    public interface IScanOperator : IGrainWithIntegerKey
    {
        Task SubmitTuples(List<Tuple> row,bool isLeft);
        Task PauseOperator();
        Task ResumeOperator();
        Task QuitOperator();
    }
}