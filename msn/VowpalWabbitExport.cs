using Microsoft.Content.Recommendations.TrainingRuntime.Context;
using Microsoft.Content.Recommendations.TrainingRuntime.Trainer;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using VW;
using VW.Serializer;

namespace msn
{
    public static class VowpalWabbitExport
    {
        private class ReferenceEqualityComparer : IEqualityComparer<object>
        {
            public bool Equals(object x, object y)
            {
                return object.ReferenceEquals(x, y);
            }

            public int GetHashCode(object obj)
            {
                return obj.GetHashCode();
            }
        }


        public static void Export(string inputDirectory, string prefix, string modelPrefix, DateTime startTimeInclusive, DateTime endTimeExclusive)
        {
            var modelDir = string.Format(@"{0}\{1}-{2:yyyyMMdd}", inputDirectory, modelPrefix, startTimeInclusive);

            var outputFile = modelDir + ".vw.gz";
            var outputDict = modelDir + ".dict.gz";

            var dictionary = new Dictionary<string, string>();
            var fastDictionary = new Dictionary<object, string>(new ReferenceEqualityComparer());
            var settings = new VowpalWabbitSettings("--cb_adf") { EnableExampleCaching = true, EnableStringFloatCompact = true };

            using (var vw = new VowpalWabbit<UserContext<DocumentFeatures>, DocumentFeatures>(settings))
            using (var output = new StreamWriter(new GZipStream(new FileStream(outputFile, FileMode.Create), CompressionLevel.Optimal)))
            {
                int exampleCount = 0;
                ReplayReader.Read(inputDirectory, prefix, modelPrefix, startTimeInclusive, endTimeExclusive,
                    readAheadMax: 150000,
                    restart: modelFile =>
                    {
                    },
                    reload: modelFile =>
                    {
                        //vw.Native.Reload();
                        //trainingSet.Clear();
                    },
                    dataFunc: (ts, modelId, data) =>
                    {
                        exampleCount++;
                        var d = (UserContext<DocumentFeatures>)data.Context;
                        var str = VowpalWabbitMultiLine.SerializeToString(
                            vw,
                            d,
                            d.ActionDependentFeatures,
                            data.Action - 1,
                            Program.CreateLabel(data),
                            dictionary,
                            fastDictionary);

                        output.WriteLine(str);
                        //trainingSet.Add(data);
                    },
                    missing: (missingEventId, fileName) =>
                    {
                        // also reset training data stack
                        //trainingSet.Clear();
                    },
                    endOfTrackback: (ts, startingModelFileName, modelFilename) =>
                    {
                        // model is not null only if a restart or reload was encountered, otherwise not reproducible
                        //if (vw != null)
                        //{
                        //    foreach (TrainingData data in trainingSet)
                        //    {
                        //        var d = (UserContext<DocumentFeatures>)data.Context;
                        //        vw.Learn(d, d.ActionDependentFeatures, data.Action - 1, Program.CreateLabel(data));
                        //    }

                        //trainingSet.Clear();
                    },
                    stop: () => exampleCount > 100
                );
            }

            using (var dictWriter = new StreamWriter(new GZipStream(new FileStream(outputDict, FileMode.Create), CompressionLevel.Optimal)))
            {
                foreach (var kv in dictionary)
                {
                    dictWriter.WriteLine(string.Format(
                        CultureInfo.InvariantCulture,
                        "{0} {1}",
                        kv.Value, kv.Key));
                }
            }
        }
    }
}
