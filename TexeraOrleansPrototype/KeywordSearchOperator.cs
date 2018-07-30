using Orleans;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace TexeraOrleansPrototype
{
    public class KeywordSearchOperator : Grain, IKeywordSearchOperator
    {
        private Guid guid = Guid.NewGuid();

        public Task<Guid> GetStreamGuid()
        {
            return Task.FromResult(guid);
        }
        public Task SubmitTuples(Tuple row) {
            Console.WriteLine("Keyword operator received the temperature");
            var streamProvider = GetStreamProvider("SMSProvider");
            //Get the reference to a stream
            // Console.WriteLine("Keyword side guid is " + guid);
            var stream = streamProvider.GetStream<Tuple>(guid, "Random");
            // await stream.OnNextAsync(temperature);

            if(row.text.Contains("rains"))
            {
                stream.OnNextAsync(row);
            }       

            return Task.CompletedTask;
            // return;
        }
    }
}