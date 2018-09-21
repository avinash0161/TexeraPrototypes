using Orleans;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace TexeraOrleansPrototype
{
    public interface IOrderedCountOperator : IOrderingGrain
    {
        Task SetAggregatorLevel(bool isIntermediate);
        Task<Guid> GetStreamGuid();
        Task SubmitIntermediateAgg(int aggregation);
        
    }

    public interface ICountOperator : INormalGrain
    {
        Task SetAggregatorLevel(bool isIntermediate);
        Task<Guid> GetStreamGuid();
        Task SubmitIntermediateAgg(int aggregation);

    }

}