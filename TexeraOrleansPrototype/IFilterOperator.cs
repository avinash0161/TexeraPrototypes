using Orleans;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace TexeraOrleansPrototype
{
    public interface IFilterOperator : IGrainWithIntegerKey
    {
        Task Process(object row, int seq_token=-2);
        Task OrderingProcess(object row, int seq_token);
    }
}