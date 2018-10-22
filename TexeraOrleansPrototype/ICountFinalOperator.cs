using Orleans;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace TexeraOrleansPrototype
{
    public interface IOrderedCountFinalOperator : IOrderingGrain
    {
        // Task SetAggregatorLevel(bool isIntermediate);
        Task<Guid> GetStreamGuid();
        Task SubmitIntermediateAgg(int aggregation);
        
    }

    public interface ICountFinalOperator : INormalGrain
    {
        // Task SetAggregatorLevel(bool isIntermediate);
        Task<Guid> GetStreamGuid();
        Task SubmitIntermediateAgg(int aggregation);

    }

}