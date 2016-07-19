using CsvHelper;
using Microsoft.Content.Recommendations.TrainingRuntime.Context;
using Microsoft.Content.Recommendations.TrainingRuntime.Trainer;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using VW;

namespace msn
{
    public class ReplayExperiment
    {
        public static void Run(string inputDirectory, string prefix, string modelPrefix, DateTime startTimeInclusive, DateTime endTimeExclusive, List<string> arguments)
        {
            var settings = arguments.Select(arg => new VowpalWabbitSettings(arg)).ToList();

            var modelDir = string.Format(@"{0}\{1}-{2:yyyyMMdd}", inputDirectory, modelPrefix, startTimeInclusive);
            var reproModelDir = modelDir + "-replay";
            Directory.CreateDirectory(reproModelDir);

            var trainingSet = new List<TrainingData>();
            var testSets = new Dictionary<string, TestSet>();
            var modelInfo = new Dictionary<string, List<ScoringResult>>();

            using (var vw = new VowpalWabbitSweep<UserContext<DocumentFeatures>, DocumentFeatures>(settings))
            {
                Action<string> restartOrReload = modelFile =>
                    {
                        // train with what he have so far and reload
                        ProcessExamples(vw, trainingSet, testSets);
                        vw.Reload();
                    };

                ReplayReader.Read(inputDirectory, prefix, modelPrefix, startTimeInclusive, endTimeExclusive,
                    readAheadMax: 150000,
                    restart: restartOrReload,
                    reload: restartOrReload,
                    dataFunc: (ts, modelId, data) =>
                    {
                        trainingSet.Add(data);
                    },
                    missing: (_,__) => { },
                    endOfTrackback: (ts, startingModelFileName, modelFilename) =>
                    {
                        string modelId;
                        using (var vwOriginal = new VowpalWabbit<UserContext<DocumentFeatures>, DocumentFeatures>(new VowpalWabbitSettings("--quiet -i " + modelFilename)))
                        {
                            // Copy ID to make models identical
                            modelId = vwOriginal.Native.ID;
                        }

                        // train
                        ProcessExamples(vw, trainingSet, testSets);

                        // save models
                        string outputModel = Path.Combine(reproModelDir, modelId);
                        var trainedModels = vw.SaveModels(outputModel);

                        var scoringResults = settings.Select((s, i) =>
                            new ScoringResult
                            {
                                Settings = s,
                                SourceModelId = modelId,
                                Model = trainedModels[i],
                                Timestamp = ts
                            })
                            .ToList();

                        modelInfo.Add(modelId, scoringResults);
                    }
                    //, stop: () => modelInfo.Count > 20
                );
            }

            var detailedCsvFilename = Path.Combine(reproModelDir, "detail.csv");

            // reporting
            using (var detailedCsv = new CsvWriter(new StreamWriter(detailedCsvFilename)))
            {
                detailedCsv.WriteField("Timestamp");
                detailedCsv.WriteField("MinEventTimestamp");
                detailedCsv.WriteField("MaxEventTimestamp");
                detailedCsv.WriteField("Model");
                detailedCsv.WriteField("Arguments");
                detailedCsv.WriteField("NumberOfExamplesPerPass");
                detailedCsv.WriteField("TotalNumberOfFeatures");
                detailedCsv.WriteField("AverageLoss");
                detailedCsv.WriteField("NumberOfExamples");
                detailedCsv.NextRecord();
            }

            var testSetProcessed = 0;
            // scoring
            Parallel.ForEach(
                testSets,
                new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount * 2 },
                ts =>
                {
                    List<ScoringResult> results;

                    if (!modelInfo.TryGetValue(ts.Key, out results))
                    {
                        // we didn't train the model yet
                        return;
                    }

                    // , enableExampleCaching: true
                    var testSettings = results.Select(r => new VowpalWabbitSettings("--quiet -t -i " + r.Model)).ToList();

                    using (var vw = new VowpalWabbitSweep<UserContext<DocumentFeatures>, DocumentFeatures>(testSettings))
                    {
                        foreach (TrainingData data in ts.Value.Data)
                        {
                            var d = (UserContext<DocumentFeatures>)data.Context;
                            var prediction = vw.Predict(d, d.ActionDependentFeatures, data.Action - 1, Program.CreateLabel(data));

                            for (int i = 0; i < results.Count; i++)
			                {
                                // count the number of predictions per slot
                                results[i].PositionStats[prediction[i][0].Index]++;
			                }
                        }

                        var minEventTimestamp = ts.Value.Data.Min(td => ((UserContext<DocumentFeatures>)td.Context).EventTime);
                        var maxEventTimestamp = ts.Value.Data.Max(td => ((UserContext<DocumentFeatures>)td.Context).EventTime);
                        for (int i = 0; i < results.Count; i++)
                        {
                            results[i].PerformanceStatistics = vw.VowpalWabbits[i].PerformanceStatistics;
                            results[i].NumberOfExamples = ts.Value.Data.Count;
                            results[i].MinEventTimestamp = (minEventTimestamp??DateTimeOffset.MinValue).UtcDateTime;
                            results[i].MaxEventTimestamp = (maxEventTimestamp ?? DateTimeOffset.MaxValue).UtcDateTime;
                        }

                        lock (detailedCsvFilename)
                        {
                            // append
                            using (var detailedCsv = new CsvWriter(new StreamWriter(detailedCsvFilename, true)))
                            {
                                // one sweep configuration
                                int sweepNr = 0;
                                foreach (var sweep in results)
	                            {
                                    if (sweep.PerformanceStatistics == null)
                                        continue;

                                    detailedCsv.WriteField(sweep.Timestamp);
                                    detailedCsv.WriteField(sweep.MinEventTimestamp);
                                    detailedCsv.WriteField(sweep.MaxEventTimestamp);
                                    detailedCsv.WriteField(sweep.SourceModelId);
                                    detailedCsv.WriteField(settings[sweepNr].Arguments);
                                    detailedCsv.WriteField(sweep.PerformanceStatistics.NumberOfExamplesPerPass);
                                    detailedCsv.WriteField(sweep.PerformanceStatistics.TotalNumberOfFeatures);
                                    detailedCsv.WriteField(sweep.PerformanceStatistics.AverageLoss);
                                    detailedCsv.WriteField(sweep.NumberOfExamples);

                                    var posStats = sweep.PositionStats.ToArray();
                                    for (int i = 0; i < posStats.Length; i++)
                                        detailedCsv.WriteField(posStats[i]);

                                    detailedCsv.NextRecord();

                                    sweepNr++;
	                            }
                            }

                            testSetProcessed++;

                            Console.WriteLine("Progress {0}/{1}", testSetProcessed, testSets.Count);
                        }
                    }
                });

            // reporting
            using (var summaryCsv = new CsvWriter(new StreamWriter(Path.Combine(reproModelDir, "summary.csv"))))
            {
                var res = from model in modelInfo
                          from sweep in model.Value
                          where sweep.PerformanceStatistics != null
                          group sweep by sweep.Settings.Arguments into g
                          select new
                          {
                              Arguments = g.Key,
                              AverageLoss =
                                g.Sum(s => s.PerformanceStatistics.NumberOfExamplesPerPass * s.PerformanceStatistics.AverageLoss) /
                                g.Sum(s => (double)s.PerformanceStatistics.NumberOfExamplesPerPass),
                              NumberOfExamples = g.Sum(s => (double)s.PerformanceStatistics.NumberOfExamplesPerPass)
                          };

                summaryCsv.WriteRecords(res);
            }
        }

        private static void ProcessExamples(VowpalWabbitSweep<UserContext<DocumentFeatures>, DocumentFeatures> vws, List<TrainingData> trainingSet, Dictionary<string, TestSet> testSets)
        {
            // train set in parallel
            vws.Execute((vw, serializer, serializerAdf, index) =>
                {
                    foreach (TrainingData data in trainingSet)
                    {
                        // train model
                        var d = (UserContext<DocumentFeatures>)data.Context;

                        VowpalWabbitMultiLine.Learn(
                            vw,
                            serializer,
                            serializerAdf,
                            d,
                            d.ActionDependentFeatures,
                            data.Action - 1,
                            Program.CreateLabel(data));
                    }
                });

            foreach (TrainingData data in trainingSet)
            {
                var d = (UserContext<DocumentFeatures>)data.Context;

                if (d.ModelId == null)
                    continue;

                // collect data for test set
                TestSet testSet;
                if (!testSets.TryGetValue(d.ModelId, out testSet))
                {
                    testSet = new TestSet();
                    testSets.Add(d.ModelId, testSet);
                }

                testSet.Data.Add(data);
            }
            trainingSet.Clear();
        }

        private class TestSet
        {
            internal List<TrainingData> Data = new List<TrainingData>();
        }

        private class ScoringResult
        {
            /// <summary>
            /// Number of predictions per slot
            /// </summary>
            internal AutoArray<int> PositionStats = new AutoArray<int>();

            internal VowpalWabbitPerformanceStatistics PerformanceStatistics;

            internal VowpalWabbitSettings Settings;

            internal string SourceModelId;

            internal string Model;

            internal DateTime Timestamp;

            internal int NumberOfExamples;

            internal DateTime MinEventTimestamp;

            internal DateTime MaxEventTimestamp;
        }
    }
}
