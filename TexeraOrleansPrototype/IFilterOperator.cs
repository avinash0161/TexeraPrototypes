using Orleans;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace TexeraOrleansPrototype
{
    public interface IOrderedFilterOperator : IOrderingGrain
    {
    }

    public interface IFilterOperator : INormalGrain
    {
    }
}