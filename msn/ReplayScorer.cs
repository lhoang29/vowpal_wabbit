using Microsoft.Content.Recommendations.TrainingRuntime.Context;
using Microsoft.Content.Recommendations.TrainingRuntime.Trainer;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using VW;
using VW.Labels;

namespace msn
{
    public class ReplayScorer
    {
        public static void Run(string inputDirectory, string prefix, string modelPrefix, DateTime? startTimeInclusive, DateTime? endTimeExclusive, float epsilon)
        {
            var modelDir = string.Format(@"{0}\{1}-{2:yyyyMMdd}", inputDirectory, modelPrefix, startTimeInclusive);

            var vws = Directory.GetFiles(modelDir)
                .Where(d => !d.Contains("trackback"))
                .Select
                (
                    d => new VowpalWabbit<UserContext<DocumentFeatures>, DocumentFeatures>
                    (
                        new VowpalWabbitSettings("--quiet -t -i " + d) { EnableExampleCaching = false }
                    )
                )
                .ToDictionary(
                    vw => vw.Native.ID,
                    vw => Tuple.Create(vw, new ActionBlock<TrainingData>
                    (
                        data =>
                        {
                            if (data.Probability > (1-epsilon))
                            {
                                var context = (UserContext<DocumentFeatures>)data.Context;
                                var prediction = vw.Predict(context, context.ActionDependentFeatures);
                                if (prediction[0].Index != data.Action - 1)
                                {
                                    Console.WriteLine("Failed {0}", data.Id);
                                }
                            }
                        },
                        new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 1 }
                    )
                ));

            var dataset = Data.Read(inputDirectory, prefix, startTimeInclusive, endTimeExclusive)
                .Where(t => t != null);

            using (var missingWriter = new StreamWriter(File.Create(Path.Combine(inputDirectory, "missing.log"))))
            {
                foreach (TrainingData data in dataset)
                {
                    var context = (UserContext<DocumentFeatures>)data.Context;

                    if (context.ModelId == null || !vws.ContainsKey(context.ModelId))
                    {
                        missingWriter.WriteLine("model id: {0}, event id: {1}", context.ModelId, data.Id);
                        continue;
                    }
                    vws[context.ModelId].Item2.Post(data);
                }
            }

        }
    }
}
