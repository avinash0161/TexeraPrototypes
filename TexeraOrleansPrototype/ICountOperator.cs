using Orleans;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Orleans.Streams;

namespace TexeraOrleansPrototype
{
    public interface ICountOperator : IGrainWithIntegerKey, IAsyncObserver<Tuple>
    {
        Task OutTo(string operator_name);
    }
}