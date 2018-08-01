using Orleans;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace TexeraOrleansPrototype
{
    public interface IKeywordSearchOperator : IGrainWithIntegerKey
    {
        Task<Guid> GetStreamGuid();

        Task SubmitTuples(Tuple row);
        Task PauseOperator();
        Task ResumeOperator();
        Task QuitOperator();
    }
}