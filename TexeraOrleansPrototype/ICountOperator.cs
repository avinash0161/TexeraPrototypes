using Orleans;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace TexeraOrleansPrototype
{
    public interface ICountOperator : IGrainWithIntegerKey
    {
        Task SetAggregatorLevel(bool isIntermediate);
        Task<Guid> GetStreamGuid();
        Task SubmitTuples(Tuple row);
        Task SubmitIntermediateAgg(int aggregation);
        Task PauseOperator();
        Task ResumeOperator();
    }
}