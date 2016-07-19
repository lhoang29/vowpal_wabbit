using CsvHelper;
using Microsoft.Content.Recommendations.TrainingRuntime.Context;
using msn_common;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using VW;
using VW.Labels;

namespace msn
{
    public static class ReplayCooked
    {
        private class ScoringTask
        {
            public List<TrainingDataOutput> TestData;

            public string Model;

            public SweepArguments Arguments;
        }

        public static void Run(string inputDirectory, string sweepName, float epsilon,
            List<SweepArguments> arguments, List<CookedFile> trainFiles = null,
            string debugVWExportFolder = null,
            bool debugUseDictFeatures = false,
            int debugNumTrainFiles = -1,
            bool debugExportOnly = true)
        {
            // initialize random generators for each to make it reproducible
            for (int i = 0; i < arguments.Count; i++)
                arguments[i].InitializeRandom();

            Dictionary<string, int> ldaToIndex = null;
            Dictionary<string, int> jsonKeyToLDAIndex = null;
            if (debugUseDictFeatures)
            {
                trainFiles = trainFiles ?? CookedFile.EnumerateDirectory(inputDirectory, epsilon, out ldaToIndex, out jsonKeyToLDAIndex);
            }
            else
            {
                trainFiles = trainFiles ?? CookedFile.EnumerateDirectory(inputDirectory, epsilon);
            }

            StreamWriter trainWriter = null;
            string outputFileTest = string.Empty;
            string debugLastTestFileName = string.Empty;
            if (debugVWExportFolder != null)
            {
                if (arguments.Count != 1)
                {
                    throw new Exception("Export to VW string for debug should only run on 1 sweep argument.");
                }
                Directory.CreateDirectory(Path.Combine(inputDirectory, debugVWExportFolder));
                Directory.CreateDirectory(Path.Combine(inputDirectory, debugVWExportFolder, "train"));
                Directory.CreateDirectory(Path.Combine(inputDirectory, debugVWExportFolder, "test"));

                var outputArgFile = Path.Combine(inputDirectory, debugVWExportFolder, "args.txt");
                var outputDictFeatureFile = Path.Combine(inputDirectory, debugVWExportFolder, "dict.vw.gz");
                var outputFileTrain = Path.Combine(inputDirectory, debugVWExportFolder, "train", "train.vw.gz");
                outputFileTest = Path.Combine(inputDirectory, debugVWExportFolder, "test", "{0}.vw.gz");

                int debugLastFileIndex = Math.Min(debugNumTrainFiles, trainFiles.Count) - 1;
                while (debugLastFileIndex >= 0 && trainFiles[debugLastFileIndex].TestFile == null)
                {
                    debugLastFileIndex--;
                }
                if (debugLastFileIndex >= 0)
                {
                    debugLastTestFileName = trainFiles[debugLastFileIndex].ModelId;
                }
                Trace.TraceInformation("Test file to be exported: {0}", debugLastTestFileName);

                if (File.Exists(outputArgFile) || File.Exists(outputFileTrain))
                {
                    throw new Exception(string.Format("Output file {0} or {1} already exists", outputArgFile, outputFileTrain));
                }

                trainWriter = new StreamWriter(new GZipStream(File.Create(outputFileTrain), CompressionLevel.Optimal));

                // write args to file to associate with serialized vw data
                File.WriteAllText(outputArgFile, arguments[0].Arguments);

                if (ldaToIndex != null)
                {
                    // write dictionary features file
                    using (var dictWriter = new StreamWriter(new GZipStream(File.Create(outputDictFeatureFile), CompressionLevel.Optimal)))
                    {
                        foreach (string ldaString in ldaToIndex.Keys)
                        {
                            dictWriter.WriteLine(string.Format("{0} {1}", ldaToIndex[ldaString], ldaString));
                        }
                    }
                }
            }

            var sweepOutDir = Path.Combine(inputDirectory, sweepName);
            var sweepOutModelDir = Path.Combine(sweepOutDir, "models");
            Directory.CreateDirectory(sweepOutDir);
            Directory.CreateDirectory(sweepOutModelDir);

            var detailedCsvFilename = Path.Combine(inputDirectory, sweepName, "detail.csv");
            WriteCsvHeader(detailedCsvFilename);

            TraceSweep(sweepName, "Processing with {0} threads", Environment.ProcessorCount);

            var settings = arguments
                .Select(arg => debugVWExportFolder == null ?
                    new VowpalWabbitSettings(arg.Arguments) :
                    new VowpalWabbitSettings(arg.Arguments) { EnableExampleCaching = false, EnableStringExampleGeneration = true })
                .ToList();
            using (var vw = new VowpalWabbitSweep<UserContext<DocumentFeatures>, DocumentFeatures>(settings))
            {
                foreach (var f in trainFiles)
                {
                    TraceSweep(sweepName, "{0} Processing block {1}/{2}", DateTime.Now, (f.Nr + 1), trainFiles.Count);

                    // deserialize lines in parallel
                    var trainSet = f.Deserialize();

                    // train models
                    TrainModels(sweepName, vw, trainSet, arguments, epsilon, trainWriter, ldaToIndex, debugExportOnly);

                    // check if test set exists
                    var testSetFile = f.TestFile;
                    if (testSetFile == null)
                        continue;

                    var testSet = testSetFile.Deserialize();
                    TraceSweep(sweepName, "{0} Test data available: {1} examples", DateTime.Now, testSet.Count);

                    // dispatch scoring tasks for each model on the same input
                    var models = vw.SaveModels(Path.Combine(sweepOutModelDir, f.ModelId));
                    Parallel.For(0, models.Count,
                        new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount },
                        i =>
                        {
                            var st = new ScoringTask
                            {
                                TestData = testSet,
                                Model = models[i],
                                Arguments = arguments[i]
                            };

                            if (debugVWExportFolder != null)
                            {
                                // only export the last test file for efficiency
                                string debugOutputTestFile = null;
                                if (!string.IsNullOrWhiteSpace(debugLastTestFileName) && debugLastTestFileName == f.ModelId)
                                {
                                    debugOutputTestFile = string.Format(outputFileTest, f.ModelId);
                                    TraceSweep(sweepName, "Found test file to export: {0}", debugOutputTestFile);
                                    TestModel(detailedCsvFilename, st, sweepName, debugOutputTestFile, ldaToIndex, debugExportOnly);
                                }
                            }
                            else
                            {
                                TestModel(detailedCsvFilename, st, sweepName);
                            }
                        });

                    if (debugNumTrainFiles > 0 && f.Nr >= debugNumTrainFiles - 1)
                    {
                        break;
                    }
                }
            }
            if (trainWriter != null)
            {
                trainWriter.Dispose();
            }
        }

        private static void TrainModels(
            string sweepName,
            VowpalWabbitSweep<UserContext<DocumentFeatures>, DocumentFeatures> vws,
            List<TrainingDataOutput> trainSet,
            List<SweepArguments> arguments,
            float epsilon,
            StreamWriter debugExportWriter = null,
            Dictionary<string, int> ldaToIndex = null,
            bool debugExportOnly = true)
        {
            TraceSweep(sweepName, "{0} Training on {1} examples {2} settings in parallel", DateTime.Now, trainSet.Count, vws.VowpalWabbits.Length);

            // train in parallel
            vws.Execute((vw, serializer, serializerAdf, index) =>
            {
                var rand = arguments[index].Random;

                VowpalWabbit<UserContext<DictionaryFeatures>, DictionaryFeatures> vwDebug = null;
                if (ldaToIndex != null)
                {
                    vwDebug = new VowpalWabbit<UserContext<DictionaryFeatures>,DictionaryFeatures>(vw.Settings);
                }

                foreach (TrainingDataOutput data in trainSet)
                {
                    var probability = data.Probability;

                    var label = new ContextualBanditLabel()
                    {
                        Action = (uint)data.Action,
                        Probability = probability,
                        Cost = data.Cost
                    };

                    if (debugExportWriter != null)
                    {
                        string vwLine = null;
                        if (ldaToIndex != null)
                        {
                            UserContext<DictionaryFeatures> newContext = CreateDictContextFromDocContext(ldaToIndex, data);

                            vwLine = VowpalWabbitMultiLine.SerializeToString(
                                vwDebug,
                                newContext,
                                newContext.ActionDependentFeatures,
                                data.Action - 1,
                                label);
                        }
                        else
                        {
                            vwLine = VowpalWabbitMultiLine.SerializeToString(
                            vw,
                            data.Context,
                            data.Context.ActionDependentFeatures,
                            data.Action - 1,
                            label);
                        }
                        debugExportWriter.WriteLine(vwLine);

                        if (debugExportOnly)
                        {
                            continue;
                        }
                    }

                    VowpalWabbitMultiLine.Learn(
                        vw,
                        serializer,
                        serializerAdf,
                        data.Context,
                        data.Context.ActionDependentFeatures,
                        data.Action - 1,
                        label);
                }
                if (vwDebug != null)
                {
                    vwDebug.Dispose();
                }
            });
        }

        private static void TestModel(
            string detailedCsvFilename,
            ScoringTask st,
            string sweepName,
            string debugOutputVWFile = null,
            Dictionary<string, int> ldaToIndex = null,
            bool debugExportOnly = true)
        {
            var fileInfo = new FileInfo(st.Model);
            var fileSize = fileInfo.Length;
            string args = "--quiet -t";
            var vwSettings = debugOutputVWFile == null ? new VowpalWabbitSettings(args) { ModelStream = File.OpenRead(st.Model) } :
                new VowpalWabbitSettings(args) { ModelStream = File.OpenRead(st.Model), EnableStringExampleGeneration = true, EnableStringFloatCompact = true };

            var debugExportWriter = string.IsNullOrWhiteSpace(debugOutputVWFile) ? null : new StreamWriter(new GZipStream(File.Create(debugOutputVWFile), CompressionLevel.Optimal));
            using (var vw = new VowpalWabbit<UserContext<DocumentFeatures>, DocumentFeatures>(vwSettings))
            {
                VowpalWabbit<UserContext<DictionaryFeatures>, DictionaryFeatures> vwDebug = null;
                if (ldaToIndex != null)
                {
                    vwDebug = new VowpalWabbit<UserContext<DictionaryFeatures>, DictionaryFeatures>(vwSettings);
                }
                foreach (var data in st.TestData)
                {
                    var d = data.Context;
                    var label = CreateLabel(data);

                    if (debugExportWriter != null)
                    {
                        string vwLine = null;
                        if (ldaToIndex != null)
                        {
                            UserContext<DictionaryFeatures> newContext = CreateDictContextFromDocContext(ldaToIndex, data);
                            vwLine = VowpalWabbitMultiLine.SerializeToString(
                                vwDebug,
                                newContext,
                                newContext.ActionDependentFeatures,
                                data.Action - 1,
                                label);
                        }
                        else
                        {
                            vwLine = VowpalWabbitMultiLine.SerializeToString(
                                vw,
                                d,
                                d.ActionDependentFeatures,
                                data.Action - 1,
                                label);
                        }
                        debugExportWriter.WriteLine(vwLine);

                        if (debugExportOnly)
                        {
                            continue;
                        }
                    }

                    vw.Predict(d, d.ActionDependentFeatures, data.Action - 1, label);
                }
                if (vwDebug != null)
                {
                    vwDebug.Dispose();
                }

                var minEventTimestamp = st.TestData.Min(td => td.Context.EventTime);
                var maxEventTimestamp = st.TestData.Max(td => td.Context.EventTime);

                lock (detailedCsvFilename)
                {
                    // append
                    using (var detailedCsv = new CsvWriter(new StreamWriter(detailedCsvFilename, true)))
                    {
                        detailedCsv.WriteField(sweepName);
                        detailedCsv.WriteField(minEventTimestamp);
                        detailedCsv.WriteField(maxEventTimestamp);
                        detailedCsv.WriteField(Path.GetFileName(st.Model));
                        detailedCsv.WriteField(st.Arguments.Arguments);
                        detailedCsv.WriteField(vw.Native.PerformanceStatistics.NumberOfExamplesPerPass);
                        detailedCsv.WriteField(vw.Native.PerformanceStatistics.TotalNumberOfFeatures);
                        detailedCsv.WriteField(vw.Native.PerformanceStatistics.AverageLoss);
                        detailedCsv.WriteField(Regex.Match(st.Arguments.Arguments, @"--cb_type ([A-z]+)").Groups[1].Value);
                        detailedCsv.WriteField(Regex.Match(st.Arguments.Arguments, @"-l ([0-9.]+)").Groups[1].Value);
                        //detailedCsv.WriteField(fileInfo.FullName);
                        //detailedCsv.WriteField(fileSize);
                        detailedCsv.NextRecord();
                    }
                }
            }
            if (debugExportWriter != null)
            {
                debugExportWriter.Dispose();
            }
        }

        private static UserContext<DictionaryFeatures> CreateDictContextFromDocContext(Dictionary<string, int> ldaToIndex, TrainingDataOutput data)
        {
            UserContext<DocumentFeatures> context = data.Context;

            // construct new context object using dictionary features
            var dictFeatures = new List<DictionaryFeatures>();
            foreach (DocumentFeatures doc in context.ActionDependentFeatures)
            {
                dictFeatures.Add(DictionaryFeatures.CreateFromDocumentFeatures(doc, ldaToIndex));
            }
            var newContext = new UserContext<DictionaryFeatures>()
            {
                User = context.User,
                UserLDAVector = context.UserLDAVector,
                ActionDependentFeatures = dictFeatures,
                ModelId = context.ModelId,
                EventTime = context.EventTime
            };
            return newContext;
        }

        private static void WriteCsvHeader(string detailedCsvFilename)
        {
            using (var detailedCsv = new CsvWriter(new StreamWriter(detailedCsvFilename)))
            {
                detailedCsv.WriteField("Run");
                detailedCsv.WriteField("MinEventTimestamp");
                detailedCsv.WriteField("MaxEventTimestamp");
                detailedCsv.WriteField("Model");
                detailedCsv.WriteField("Arguments");
                detailedCsv.WriteField("NumberOfExamplesPerPass");
                detailedCsv.WriteField("TotalNumberOfFeatures");
                detailedCsv.WriteField("AverageLoss");
                detailedCsv.WriteField("cb");
                detailedCsv.WriteField("l");
                detailedCsv.NextRecord();
            }
        }

        public static ContextualBanditLabel CreateLabel(TrainingDataOutput data)
        {
            return new ContextualBanditLabel()
            {
                Action = (uint)data.Action,
                Probability = data.Probability,
                Cost = data.Cost
            };
        }

        public static void ExportDictionaryFile(string vwDataFile)
        {
            string dummy1, dummy2;
            ExportDictionaryFile(vwDataFile, out dummy1, out dummy2);
        }

        public static void ExportDictionaryFile(string vwDataFile, out string vwDictDataFile, out string vwDictFeatureFile)
        {
            // TODO: quick hack to create dictionary features, this should be baked into VW serialization instead
            int index = 0;
            var dict = new Dictionary<string, int>();
            var lines = new List<string>();
            string ldaSeparator = "doclda";

            string vwStringDirectory = Path.GetDirectoryName(vwDataFile);
            vwDictDataFile = Path.Combine(vwStringDirectory, "dict_data_" + Path.GetFileName(vwDataFile));
            vwDictFeatureFile = Path.Combine(vwStringDirectory, "dict_features_" + Path.GetFileName(vwDataFile));

            using (var sr = new StreamReader(File.OpenRead(vwDataFile)))
            {
                while (!sr.EndOfStream)
                {
                    string l = sr.ReadLine();
                    if (l.StartsWith("shared"))
                    {
                        lines.Add(l);
                        continue;
                    }
                    int ldaIndex = l.IndexOf(ldaSeparator);
                    if (ldaIndex > 0)
                    {
                        string ldaString = l.Substring(ldaIndex + ldaSeparator.Length);
                        if (!dict.ContainsKey(ldaString))
                        {
                            dict.Add(ldaString, index);
                            index++;
                        }
                        lines.Add(l.Substring(0, ldaIndex) + " " + dict[ldaString]);
                    }
                    else
                    {
                        lines.Add(l);
                    }
                }
            }

            // write new train file with dict features reference
            using (var sw = new StreamWriter(new GZipStream(File.Create(vwDictDataFile), CompressionLevel.Optimal)))
            {
                for (int i = 0; i < lines.Count; i++)
                {
                    sw.WriteLine(lines[i]);
                }
            }

            // write actual dictionary features
            using (var sw = new StreamWriter(new GZipStream(File.Create(vwDictFeatureFile), CompressionLevel.Optimal)))
            {
                foreach (string ldaString in dict.Keys)
                {
                    sw.WriteLine(string.Format("{0} {1}", dict[ldaString], ldaString));
                }
            }
        }

        public static void TraceSweep(string sweepName, string format, params object[] args)
        {
            Trace.TraceInformation(sweepName + " - " + format, args);
        }
    }
}
