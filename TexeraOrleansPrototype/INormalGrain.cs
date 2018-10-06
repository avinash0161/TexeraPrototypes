using Orleans;
using Orleans.Streams;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace TexeraOrleansPrototype
{
    public interface INormalGrain : IGrainWithIntegerKey,IAsyncObserver<object>
    {
        Task OutTo(string operator_name, bool empty_guid=false);
    }
}
