using Orleans;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Orleans.Streams;

namespace TexeraOrleansPrototype
{
    interface ICountFinalOperator: IGrainWithIntegerKey, IAsyncObserver<int>
    {
        Task OutTo(string operator_name);
    }
}
