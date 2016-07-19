using CsvHelper;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace msn
{
    public static class MarginCooked
    {
        public static void Run(string inputDirectory, float epsilon)
        {
            var trainFiles = CookedFile.EnumerateDirectory(inputDirectory, epsilon);

            var csvFilename = Path.Combine(inputDirectory, "margin-on-test.csv");
            WriteCsvHeader(csvFilename);

            int processed = 0;
            trainFiles.Where(t => t.TestFile != null)
                .AsParallel()
                .WithDegreeOfParallelism(4)
                .ForAll(f =>
                {
                    var localProcessed = Interlocked.Increment(ref processed);
                    Console.WriteLine("{0} Processing block {1}/{2}", DateTime.Now, localProcessed, trainFiles.Count);

                    var testSet = f.TestFile.Deserialize();

                    var numExplore = 0;
                    var minEventTimestamp = DateTime.MaxValue;
                    var maxEventTimestamp = DateTime.MinValue;
                    var clicksExplore = new AutoArray<int>();

                    foreach (var data in testSet)
                    {
                        if (data.IsExplore)
                        {
                            numExplore++;

                            if (data.Context.EventTime.HasValue)
                            {
                                if (minEventTimestamp > data.Context.EventTime.Value.DateTime)
                                    minEventTimestamp = data.Context.EventTime.Value.DateTime;

                                if (maxEventTimestamp < data.Context.EventTime.Value.DateTime)
                                    maxEventTimestamp = data.Context.EventTime.Value.DateTime;
                            }

                            // check if clicked
                            if (data.Cost == -1)
                            {
                                clicksExplore[data.Action - 1]++;
                            }
                        }
                    }

                    lock (csvFilename)
                    {
                        using (var detailedCsv = new CsvWriter(new StreamWriter(csvFilename, true)))
                        {
                            detailedCsv.WriteField(minEventTimestamp);
                            detailedCsv.WriteField(maxEventTimestamp);
                            detailedCsv.WriteField(testSet.Count - numExplore);
                            detailedCsv.WriteField(numExplore);

                            var clicksExploreArray = clicksExplore.ToArray();
                            detailedCsv.WriteField(clicksExploreArray.Sum());

                            foreach (var clickCount in clicksExploreArray)
                                detailedCsv.WriteField(clickCount);

                            detailedCsv.NextRecord();
                        }
                    }
                });
        }

        private static void WriteCsvHeader(string detailedCsvFilename)
        {
            using (var detailedCsv = new CsvWriter(new StreamWriter(detailedCsvFilename)))
            {
                detailedCsv.WriteField("MinEventTimestamp");
                detailedCsv.WriteField("MaxEventTimestamp");
                detailedCsv.WriteField("NumExploitExamples");
                detailedCsv.WriteField("NumExploreExamples");
                detailedCsv.WriteField("NumExploreClicks");
                detailedCsv.NextRecord();
            }
        }
    }
}
