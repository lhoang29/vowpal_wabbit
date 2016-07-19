using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Content.Recommendations.TrainingRuntime.Reader;
using Microsoft.Content.Recommendations.TrainingRuntime.RewardFunction;
using Microsoft.Content.Recommendations.TrainingRuntime.ContextLocal;
using VW.Labels;
using VW;
using Microsoft.Content.Recommendations.TrainingRuntime.Trainer;
using System.Diagnostics;
using System.IO;
using Newtonsoft.Json;
using System.IO.Compression;
using VW.Serializer;
using System.Globalization;
using System.Collections;
using Microsoft.Content.Recommendations.TrainingRuntime.Test;
using Microsoft.Content.Recommendations.TrainingRuntime.Context;
using System.Text.RegularExpressions;
using System.Threading.Tasks.Dataflow;
using Microsoft.Hadoop.Avro;
using Microsoft.Hadoop.Avro.Container;
using msn.Tasks;
using System.Threading;
using msn_common;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using System.Collections.Concurrent;

namespace msn
{
    public class Program
    {
        static void Main(string[] args)
        {
            Trace.Listeners.Add(new ConsoleTraceListener());

            if (args.Length == 5 && args[0] == SweepMetadata.Command)
            {
                string appInsightsKey = "35b47c69-c1d8-47ec-ac36-3bbaeb56f4ba"; // msnexpinsights
                Trace.Listeners.Add(new Microsoft.ApplicationInsights.TraceListener.ApplicationInsightsTraceListener(appInsightsKey));
                Program.AzureSweep(Convert.ToInt32(args[1]), Convert.ToInt32(args[2]), args[3], args[4], epsilon: 0.3333f);
                //Program.AzureSweep(1, 5, "msnexpstorage", "GYxbkis9Rw/FUfnIHTop7oWv7Gmrqww6m56EMVyST6KtzPPDHsagCpOcAKI2zhMMdgo9Vl1KxPyurPjYELMGPA==", epsilon: 0.3333f);
                return;
            }
            //var files = Directory.EnumerateFiles(@"D:\msn\westus-enusinfopanea-archive-20160622-cooked\advanced-exploration\models").ToList();
            //foreach (var f in files)
            //{
            //    using (var vw = new VowpalWabbit($"-i {f}"))
            //    { }
            //}

            //var cb = new ConcurrentBag<object>();
            //Parallel.For(0, files.Count, i =>
            //{
            //    var vw = new VowpalWabbit($"-i {files[i]}");
            //    cb.Add(vw);
            //});
            //return;
            //Console.WriteLine("Reading dictionary...");
            //Dictionary<string, DocumentFeatures> documentLDAVectorDict;
            //using (var dictReader = new JsonTextReader(new StreamReader(
            //    new GZipStream(new FileStream(Path.Combine(@"D:\msn\eastus-enusriver-archive-20151118-cooked", "dict.json.gz"), FileMode.Open), 
            //        CompressionMode.Decompress))))
            //{
            //    documentLDAVectorDict = JsonSerializer.Create().Deserialize<Dictionary<string, DocumentFeatures>>(dictReader);
            //}

            //Console.WriteLine("Check if same doc id has same features");
            //var docIdToFeatures = new Dictionary<string, float[]>();
            //foreach (var item in documentLDAVectorDict)
            //{
            //    string key = item.Value.Id + item.Value.LastModified.ToString();
            //    if (!docIdToFeatures.ContainsKey(key))
            //    {
            //        docIdToFeatures.Add(key, item.Value.LDAVector.Vectors);
            //    }
            //    else
            //    {
            //        if (!docIdToFeatures[key].SequenceEqual(item.Value.LDAVector.Vectors))
            //        {
            //            Console.WriteLine("fail");
            //        }
            //    }
            //}
            //return;
            //int numLines = 10;
            //var lines = new List<string>();
            //using (var sr = new StreamReader(File.OpenRead(@"D:\msn\eastus-enusriver-archive-20151118-cooked\out-vw-string\serialized.vw")))
            //{
            //    while (numLines > 0)
            //    {
            //        lines.Add(sr.ReadLine());
            //        numLines--;
            //    }
            //}
            //File.WriteAllLines(@"D:\msn\eastus-enusriver-archive-20151118-cooked\out-vw-string\serialized-10.vw", lines);
            //return;

            //using (var sr   = new StreamReader(File.OpenRead(@"C:\Lab\eastus-enusrivercomplete.json\eastus-enusrivercomplete.json")))
            //{
            //    Console.WriteLine(sr.ReadLine());
            //}
            //return;

            var sw = new Stopwatch();
            sw.Start();

            //VowpalWabbitExport.Export(@"C:\Data", "eastus-enusrivercomplete", "eastus-enusriver-archive", new DateTime(2015, 11, 18), new DateTime(2015, 11, 23));
            //VowpalWabbitExport.Export(@"C:\Data", "eastus-enusrivercomplete", "eastus-enusriver-archive", new DateTime(2015, 11, 18), new DateTime(2015, 11, 19));

            //var settings = new[]
            //{
            //    " -l 0.005 --cb_type mtr",
            //    "--quiet --cb_adf --rank_all --interact ud -l 0.005 --cb_type dr"
            //};
            //ThreadPool.SetMaxThreads(128, 512);

            var epsilons = new[] { .33333f, .2f, .1f, .05f }.Select(a => "--epsilon " + a);
            var bags = new[] { 1, 2, 4, 6, 8, 10 }.Select(a => "--bag " + a);
            var softmaxes = new[] { 0, 1, 2, 4, 8, 16, 32 }.Select(a => "--softmax --lambda " + a);
            //var epsilons = new[] { .33333f }.Select(a => "--epsilon " + a);
            //var bags = new[] { 1 }.Select(a => "--bag " + a);
            //var softmaxes = new[] { 0 }.Select(a => "--softmax --lambda " + a);

            var arguments = Util.Expand(
                //new[] { "--id 1" }, //, "--id 4", "--id 5", "--id 6" },
                epsilons.Union(bags).Union(softmaxes),
                new[] { "--cb_type ips", "--cb_type mtr", "--cb_type dr" },
                new[] { 0.005, 0.01, 0.02, 0.1 }.Select(l => string.Format(CultureInfo.InvariantCulture, "-l {0}", l))
                //new[] { 0.005 }.Select(l => string.Format(CultureInfo.InvariantCulture, "-l {0}", l))
                //new[] { 0.5, 0.1, 0.01, 0.001 }.Select(l => string.Format(CultureInfo.InvariantCulture, "-l {0}", l))
                //new[] { 0.01 }.Select(l => string.Format(CultureInfo.InvariantCulture, "-l {0}", l))
            )
            .Select(a => $"--cb_explore_adf {a} --interact ud ")
            .ToList();

            var settings = arguments.Select(arg => new SweepArguments { Arguments = arg }).ToList();


            //var headers = Data.ReadHeader(@"E:\mwt\msn\data\", "eastus-enusinfopanemcomplete", new DateTime(2016, 6, 30), new DateTime(2016, 7, 1));
            //for (int i = 1; i < 50; i++)
            //{
            //    BasicStats.EvalOfDefaultPolicy(headers, policyAction: i);
            //}
            //return;

            // settings = settings.Union(settings.Select(arg => new SweepArguments { Arguments = arg.Arguments }));
            // 
            //MarginCooked.Run(@"D:\msn\eastus-enusriver-archive-20151118-cooked", .3333f);
            //llayCooked.ExportDictionaryFile(@"D:\msn\eastus-enusriver-archive-20151118-cooked\out-vw-string");
            //ReplayCompareWithCommandLineVW(settings.ToList(), debugNumTrainFiles: 10, debugExportOnly: false);
            //LogCooking.Run(@"E:\mwt\msn\data", "eastus-enusinfopanemcomplete", "eastus-enusinfopanem-archive", new DateTime(2016, 6, 30), new DateTime(2016, 7, 1));
            ReplayCooked.Run(@"D:\msn\westus-enusinfopanea-archive-20160622-cooked", "advanced-exploration", .3333f, settings.ToList());
            //debugVWExportFolder: "vw-string",
            //debugUseDictFeatures: true,
            //debugNumTrainFiles: 1000,
            //debugExportOnly: true);

            // ReplayExperiment.Run(@"C:\Data", "eastus-enusrivercomplete", "eastus-enusriver-archive", new DateTime(2015, 11, 18), new DateTime(2015, 11, 19), settings);
            // ReplayExperiment.Run(@"C:\Data", "eastus-enusrivercomplete", "eastus-enusriver-archive", new DateTime(2015, 11, 19), new DateTime(2015, 11, 23), settings);

            // ReplayCooked.Run(@"C:\Data\eastus-enusriver-archive-20151118-cooked", "all", settings);

            // AzureDownloader.Download(@"C:\Data", "eastus-enusriver", new DateTime(2015, 11, 19), new DateTime(2015, 11, 25));
            // LogCooking.Run(@"C:\Data", "eastus-enusrivercomplete", "eastus-enusriver-archive", new DateTime(2015, 11, 19), new DateTime(2015, 11, 24));

            //ReplayCooked.Run(@"C:\Data\eastus-enusriver-archive-20151119-cooked", "all", settings);

            // ReplayScorer.Run(@"C:\Data", "eastus-enusrivercomplete", "eastus-enusriver-archive", new DateTime(2015, 11, 15), new DateTime(2015, 11, 18), .3333f);
            //ReplayTrainer.Run(@"C:\Data", "eastus-enusrivercomplete", "eastus-enusriver-archive", new DateTime(2015, 11, 15), new DateTime(2015, 11, 18));

            //Data.GenerateFolderStructure(@"d:\msn\eastus-enusrivercomplete20151022", "eastus-enusrivercomplete20151022", 0, 24);
            //Data.GenerateFolderStructure(@"d:\msn\eastus-enusrivercomplete20151016", "eastus-enusrivercomplete20151016", 0, 24);
            //return;
            //var header = ReadHeader();
            //WriteCSV(header);

            //CountPerDocument(header);
            //EvalOfDefaultPolicy(header);
            //CostTable(header);
            //Count(header);

            //Console.WriteLine("Action,Prob");
            //foreach (var d in header)
            //{
            //    Console.WriteLine("{0,2},{1}", d.Action, d.Probability);
            //}
            //Convert();

            //GenerateFolderStructure(@"C:\Data\eastus-enusrivercomplete20150912", "eastus-enusrivercomplete20150912", 5, 24);
            // LearnAndTest(@"C:\Data\eastus-enusrivercomplete20150912", "eastus-enusrivercomplete", startHour: 5, endHour: 23);

            //CountClicks(@"C:\Data\eastus-enusrivercomplete20150912", "eastus-enusrivercomplete", startHour: 5, endHour: 23);
            //LearnSplit(@"C:\Data\eastus-enusrivercomplete20150912", "eastus-enusrivercomplete", startHour: 5, endHour: 23);
            //LearnAndTestThreadedSweep(@"C:\Data\eastus-enusrivercomplete20150912", "eastus-enusrivercomplete", startHour: 5, endHour: 23);
            //LearnAndTestEditiorialPosition(@"C:\Data\eastus-enusrivercomplete20150912", "eastus-enusrivercomplete", startHour: 5, endHour: 23);

            ////// HEADERS
            //ClickThroughRates.ExportHeaders(@"C:\Data", "eastus-enusrivercomplete", .3333f, new DateTime(2015, 10, 29), new DateTime(2015, 10, 30));
            //ClickThroughRates.ExportHeaders(@"C:\Data", "westus-enusrivercomplete", .3333f, new DateTime(2015, 10, 29), new DateTime(2015, 10, 30));

            // ExportHeaders(@"C:\Data", "westus-enusrivercomplete", new DateTime(2015, 10, 15), new DateTime(2015, 10, 17));
            // ConvertToString(@"C:\Data", "eastus-enusrivercomplete", new DateTime(2015, 10, 16), new DateTime(2015, 10, 17));

            //ReproValidate(@"C:\Data", "eastus-enusrivercomplete", "eastus-enusriver-archive", new DateTime(2015, 10, 16), new DateTime(2015, 10, 17));
            //SID: LOUIE, uncomment the below line when working on training reproducibility
            //LearnReproModel(@"C:\Data", "eastus-enusrivercomplete", "eastus-enusriver-archive", 0, new DateTime(2015, 10, 15), new DateTime(2015, 10, 16));

            //LearnReproHyper(@"d:\msn", "eastus-enusrivercomplete", "eastus-enusriver-archive", new DateTime(2015, 10, 20), new DateTime(2015, 10, 21));
            //ExportE4(@"C:\Data", "eastus-enusrivercomplete", "eastus-enusriver-archive", new DateTime(2015, 10, 16), new DateTime(2015, 10, 17));

            //ReadNonEmpty(@"d:\msn\eastus-enusrivercomplete20151020", "eastus-enusrivercomplete", 5);
            //LearnAndTestSingle(@"C:\Data\eastus-enusrivercomplete20150912", "eastus-enusrivercomplete", startHour: 5, endHour: 23);
            //LearnAndTest();
            //ExtractHeader();

            // ModelTest.Run();

            //LearnReproModel(@"C:\Data", "eastus-enusrivercomplete", "eastus-enusriver-archive", new DateTime(2015, 10, 16), new DateTime(2015, 10, 17));
            //EvalReproModel(@"C:\Data", "eastus-enusrivercomplete", "eastus-enusriver-archive", new DateTime(2015, 10, 15), new DateTime(2015, 10, 20));

            Console.WriteLine("Done: " + sw.Elapsed);
            //Console.ReadKey();
        }

        private static void ReplayCompareWithCommandLineVW(List<SweepArguments> settings, int debugNumTrainFiles = 1, bool debugExportOnly = true)
        {
            string vwExe = null;
            string basePath = null;

            switch (Environment.MachineName.ToLower())
            {
                case "lhoang2":
                    vwExe = @"D:\Git\vw-markus\vowpal_wabbit\vowpalwabbit\x64\Release\vw.exe";
                    basePath = @"D:\msn\eastus-enusriver-archive-20151118-cooked";
                    break;
            }

            // Run replay cooked with 1 single sweep argument, outputting a single model file
            // and serialize train & test data to VW string format
            string vwExportFolderName = "out-vw-string";
            string sweepOutputFolderName = "mtr-005-eps-05";
            ReplayCooked.Run(basePath, sweepOutputFolderName, .3333f, settings,
                debugVWExportFolder: vwExportFolderName, debugNumTrainFiles: debugNumTrainFiles, debugExportOnly: debugExportOnly);


            // Run VW command line on string format files
            string directory = Path.Combine(basePath, vwExportFolderName);
            var trainFile = Path.Combine(directory, "train", "train.vw.gz");
            var testFile = Directory.GetFiles(Path.Combine(directory, "test"))
                .Select(f => new FileInfo(f))
                .OrderByDescending(f => f.CreationTime)
                .First();

            var modelDir = Path.Combine(directory, "models");
            Directory.CreateDirectory(modelDir);

            string modelFile = Path.Combine(modelDir, "vwstring.model");
            {
                // Test data
                var p = new Process();
                p.StartInfo = new ProcessStartInfo
                {
                    FileName = vwExe,
                    UseShellExecute = false,
                    RedirectStandardOutput = true,
                    Arguments = string.Format("{0} -d {1} -f {2}", 
                        settings[0].Arguments.Replace("--quiet", ""),
                        trainFile,
                        modelFile)
                };
                Console.WriteLine("----------------\n" + p.StartInfo.Arguments + "\n----------------\n");
                p.Start();
                p.WaitForExit();
            }

            {
                // Test data
                var p = new Process();
                p.StartInfo = new ProcessStartInfo
                {
                    FileName = vwExe,
                    UseShellExecute = false,
                    RedirectStandardOutput = true,
                    Arguments = string.Format("-t -i {0} -d {1}", modelFile, testFile)
                };
                Console.WriteLine("----------------\n" + p.StartInfo.Arguments + "\n----------------\n");
                p.Start();
                p.WaitForExit();
            }
            
        }

        /// <summary>
        /// Take an input file which is serialized data from MSN blobs, sample the first N lines,
        /// then generate dictionary features files. Run VW on both the original data as well as
        /// dictionary files to compare results.
        /// </summary>
        private static void GenerateDictionaryDataForDebugging(string basePath)
        {
            int line = 8192 * 2;
            string originalFile = Path.Combine(basePath, "train.vw.gz");
            string truncDataFile = Path.Combine(basePath, "train_" + line + ".vw.gz");
            string truncModelFile = Path.Combine(basePath, "train_" + line + ".vw.model");

            string truncDictDataFile = null;
            string truncDictFeatureFile = null;
            string truncDictModelFile = Path.Combine(basePath, "dict_train_" + line + ".vw.model");

            if (!File.Exists(truncDataFile))
            {
                using (var r = new StreamReader(new GZipStream(File.OpenRead(originalFile), CompressionMode.Decompress)))
                using (var w = new StreamWriter(new GZipStream(File.Create(truncDataFile), CompressionLevel.Optimal)))
                {
                    while (line >= 0 && !r.EndOfStream)
                    {
                        w.WriteLine(r.ReadLine());
                        line--;
                    }
                }
                ReplayCooked.ExportDictionaryFile(truncDataFile, out truncDictDataFile, out truncDictFeatureFile);
            }

            string vwExe = @"D:\Git\vw-louie\vowpal_wabbit\vowpalwabbit\x64\Release\vw.exe";
            string cbFlags = "--cb_type mtr -l 0.005";
            string vwArgs = "-d {0} --cb_adf --rank_all --interact {1} {2} -f {3}";
            {
                var p = new Process();
                p.StartInfo = new ProcessStartInfo
                {
                    FileName = vwExe,
                    UseShellExecute = false,
                    RedirectStandardOutput = true,
                    Arguments = string.Format(vwArgs, truncDataFile, "ud", cbFlags, truncModelFile)
                };
                Console.WriteLine("----------------\n" + p.StartInfo.Arguments + "\n----------------\n");
                p.Start();
                p.WaitForExit();
            }

            {
                var p = new Process();
                p.StartInfo = new ProcessStartInfo
                {
                    FileName = vwExe,
                    UseShellExecute = false,
                    RedirectStandardOutput = true,
                    Arguments = string.Format(vwArgs, truncDictDataFile, "u‡", cbFlags, truncDictModelFile) +
                            string.Format(" --dictionary d:{0} --dictionary_path {1} --ignore d", Path.GetFileName(truncDictFeatureFile), Path.GetDirectoryName(truncDictFeatureFile))
                };
                Console.WriteLine("----------------\n" + p.StartInfo.Arguments + "\n----------------\n");
                p.Start();
                p.WaitForExit();
            }
        }

        private static void CountEventIds()
        {
            var files = Directory.GetFiles(@"D:\msn\eastus-enusriver-archive-20151118", "*trackback.deployed");
            int totalEventCount = 0;
            Parallel.ForEach(files, new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount }, f =>
            {
                int eventCount = File.ReadAllLines(f).Count(l => IsEventId(l));
                Interlocked.Add(ref totalEventCount, eventCount);
            });
            Console.WriteLine("Total event count = {0}", totalEventCount);
        }

        static bool IsEventId(string s)
        {
            int separator = s.IndexOf('-');
            if (separator > 0)
            {
                Guid outGuid;
                return Guid.TryParse(s.Substring(separator + 1), out outGuid);
            }
            return false;
        }

        static void AzureSweep(int nodeId, int numSweeps, string storageAccountName, string storageAccountKey, float epsilon)
        {
            try
            {
#if DEBUG
                var sweepArgs = JsonConvert.DeserializeObject<List<SweepArguments>>(File.ReadAllText(SweepMetadata.SweepJsonFile));
                int numBlobs = File.ReadAllLines(SweepMetadata.BlobListFile).Length;
                Console.WriteLine("Task {0} using storage {1} and key {2}, # sweeps {3}, # blobs {4}, arg {5}, # threads {6}, machine name {7}",
                    nodeId, storageAccountName, storageAccountKey, sweepArgs.Count, numBlobs, sweepArgs[nodeId].Arguments,
                    numSweeps, Environment.MachineName);
#endif
                Trace.TraceInformation("Node {0} Sweeps {1} Machine {2}", nodeId, numSweeps, Environment.MachineName);

                Directory.CreateDirectory(Path.Combine(SweepMetadata.LocalDataDir, SweepMetadata.TrainPath));
                Directory.CreateDirectory(Path.Combine(SweepMetadata.LocalDataDir, SweepMetadata.TestPath));

                // Download blobs in order so that train data is available first
                List<string> blobNames = File.ReadAllLines(SweepMetadata.BlobListFile).ToList();
                List<string> testNames = blobNames.Where(n => n.StartsWith("test")).ToList();
                List<string> trainNames = blobNames
                    .Where(n => n.StartsWith("train"))
                    .OrderBy(f => int.Parse(new String(f.Substring(f.IndexOf('/') + 1).TakeWhile(Char.IsDigit).ToArray())))
                    .ToList();
                var downloadOrder = new List<string>();
                foreach (string trainName in trainNames)
                {
                    if (trainName.Contains(SweepMetadata.TrainPath))
                    {
                        string testBlobName = SweepMetadata.TestPath + "/" + trainName.Substring(trainName.IndexOf('-') + 1);
                        if (blobNames.Contains(testBlobName))
                        {
                            // Only add train files that have matching test files for now
                            downloadOrder.Add(trainName);
                            downloadOrder.Add(testBlobName);
                        }
                        else
                        {
                            downloadOrder.Add(trainName);
                            downloadOrder.Add(null);
                        }
                    }
                }

                var cred = new StorageCredentials(storageAccountName, storageAccountKey);
                var storageAccount = new CloudStorageAccount(cred, "core.windows.net", useHttps: true);
                CloudBlobClient client = storageAccount.CreateCloudBlobClient();
                CloudBlobContainer container = client.GetContainerReference(SweepMetadata.DataRootContainer);

                var blobRequestOptions = new BlobRequestOptions
                {
                    ServerTimeout = TimeSpan.FromSeconds(60),
                    MaximumExecutionTime = TimeSpan.FromSeconds(240),
                    RetryPolicy = new ExponentialRetry(TimeSpan.FromSeconds(3), 3),
                };
                Action<string, CloudBlobContainer, string> DownloadBlob = (path, cbContainer, cbBlobName) =>
                {
                    CloudBlockBlob blob = cbContainer.GetBlockBlobReference(cbBlobName);
                    if (!File.Exists(path))
                    {
                        using (var ms = new MemoryStream())
                        {
                            blob.DownloadToStream(ms, options: blobRequestOptions);
                            File.WriteAllBytes(path, ms.ToArray());
                            Trace.TraceInformation("Node {0} Sweeps {1} Machine {2}, Downloaded {3}", nodeId, numSweeps, Environment.MachineName, path);
                        }
                    }
                    else
                    {
                        Trace.TraceInformation("Node {0} Sweeps {1} Machine {2}, File already downloaded {3}", nodeId, numSweeps, Environment.MachineName, cbBlobName);
                    }
                };
                Action<string, CloudBlobContainer, string> UploadBlob = (path, cbContainer, cbBlobName) =>
                {
                    CloudBlockBlob blob = cbContainer.GetBlockBlobReference(cbBlobName);
                    if (File.Exists(path))
                    {
                        blob.UploadFromFile(path, FileMode.Open, options: blobRequestOptions);
                    }
                };

                // TODO: use Rx
                Task.Run(() =>
                {
                    // Download blobs asynchronously
                    foreach (string blobName in downloadOrder)
                    {
                        if (blobName != null)
                        {
                            DownloadBlob(Path.Combine(SweepMetadata.LocalDataDir, blobName), container, blobName);
                        }
                    }
                });

                Trace.TraceInformation("Node {0} Sweeps {1} Machine {2}: Loading Dictionary ...", nodeId, numSweeps, Environment.MachineName);

                // Download dictionary and create a list of cooked files based on the list above.
                string localDictFile = Path.Combine(SweepMetadata.LocalDataDir, SweepMetadata.CookedDictionaryFile);
                DownloadBlob(localDictFile, container, SweepMetadata.CookedDictionaryFile);
                var documentLDAVectorDict = new Dictionary<string, DocumentFeatures>();
                using (var dictReader = new JsonTextReader(new StreamReader(new GZipStream(new FileStream(localDictFile, FileMode.Open), CompressionMode.Decompress))))
                {
                    documentLDAVectorDict = JsonSerializer.Create().Deserialize<Dictionary<string, DocumentFeatures>>(dictReader);
                }
                var resolver = new OutputCachingReferenceResolver(documentLDAVectorDict.ToDictionary(kv => kv.Key, kv => (object)kv.Value));
                var jsonSettings = new JsonSerializerSettings() { ReferenceResolverProvider = () => resolver };

                var trainFiles = new List<CookedFile>();
                for (int i = 0; i < downloadOrder.Count; i += 2)
                {
                    string trainBlobName = downloadOrder[i];
                    string testBlobName = downloadOrder[i + 1];
                    Match regexMatch = Regex.Match(trainBlobName, @"(\d+)-(\d+)-(.+)\.gz$");
                    string modelId = regexMatch.Groups[3].Value;
                    int nr = int.Parse(regexMatch.Groups[1].Value);

                    trainFiles.Add(new CookedFile(jsonSettings, epsilon, SweepMetadata.LocalDataDir)
                    {
                        File = Path.Combine(SweepMetadata.LocalDataDir, trainBlobName),
                        Nr = nr,
                        ModelId = modelId,
                        TestFile = testBlobName == null ? null : new CookedFile(jsonSettings, epsilon, SweepMetadata.LocalDataDir)
                        {
                            File = Path.Combine(SweepMetadata.LocalDataDir, testBlobName),
                            ModelId = modelId,
                        }
                    });
                }
                Trace.TraceInformation("Node {0} Sweeps {1} Machine {2}: Running ...", nodeId, numSweeps, Environment.MachineName);

                var arguments = JsonConvert.DeserializeObject<List<SweepArguments>>(File.ReadAllText(SweepMetadata.SweepJsonFile));
                List<SweepArguments> argsToSweep = arguments.Skip(nodeId * numSweeps).Take(numSweeps).ToList();

                string sweepName = "azure-batch-sweep-" + nodeId;
                string sweepOutDir = Path.Combine(SweepMetadata.LocalDataDir, sweepName);
                ReplayCooked.Run(SweepMetadata.LocalDataDir, sweepName, epsilon, argsToSweep, trainFiles.OrderBy(f => f.Nr).ToList());

                // Upload models and header file to Azure blobs
                string[] files = Directory.GetFiles(sweepOutDir, "*", SearchOption.AllDirectories);
                Parallel.ForEach(files, new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount }, file =>
                {
                    UploadBlob(file, container, sweepName + "/" + Path.GetFileName(file));
                });
                Trace.TraceInformation("Node {0} Sweeps {1} Machine {2}: Finished.", nodeId, numSweeps, Environment.MachineName);
            }
            catch (Exception ex)
            {
                Trace.TraceError(ex.ToString());
            }
        }

        public static ContextualBanditLabel CreateLabel(TrainingData data)
        {
            return new ContextualBanditLabel()
            {
                Action = (uint)data.Action,
                Probability = data.Probability,
                Cost = data.Cost
            };
        }

        static List<TrainingData> ReadNonEmpty(string inputDirectory, string prefix, int hour)
        {
           return Data.Read(inputDirectory, hour.ToString().PadLeft(2, '0') + "-" + prefix)
                    .Where(d => d != null)
                     .Select(data =>
                     {
                         var d = (UserContext<DocumentFeatures>)data.Context;
                         for (int i = 0; i < d.ActionDependentFeatures.Count; i++)
                             d.ActionDependentFeatures[i].EditorialPosition = i;

                         return data;
                     })
                    .ToList();
        }

        static void LearnAndTestSingle(string inputDirectory, string prefix, int startHour, int endHour)
        {
            var dataSet = ReadNonEmpty(inputDirectory, prefix, 5);

            using (var vw = new VowpalWabbit<UserContext<DocumentFeatures>, DocumentFeatures>(new VowpalWabbitSettings(@"--cb_adf --rank_all --interact ud -l 1 --cb_type ips") { EnableExampleCaching = false }))
            {
                foreach (var data in dataSet)
                {
                    var d = (UserContext<DocumentFeatures>)data.Context;
                    //d.User.Location = d.User.Location.Replace(' ', '_');
                    vw.Learn(d, d.ActionDependentFeatures, data.Action - 1, CreateLabel(data));
                }
                vw.Native.SaveModel(@"c:\temp\debug.model");
            }

            dataSet = ReadNonEmpty(inputDirectory, prefix, 6);

            using (var vw = new VowpalWabbit<UserContext<DocumentFeatures>, DocumentFeatures>(new VowpalWabbitSettings(@"-t -i c:\temp\debug.model") { EnableExampleCaching = false }))
            {
                foreach (var data in dataSet)
                {
                    var d = (UserContext<DocumentFeatures>)data.Context;
                    //d.User.Location = d.User.Location.Replace(' ', '_');
                    vw.Predict(d, d.ActionDependentFeatures, data.Action - 1, CreateLabel(data));
                }
            }
        }

        static void CountClicks(string inputDirectory, string prefix, int startHour, int endHour)
        {
            var clicks = 0;
            var numExamples = 0;
            for (int hour = startHour; hour < endHour; hour++)
            {
                var dataSet = Data.Read(inputDirectory, hour.ToString().PadLeft(2, '0') + "-" + prefix)
                    .Where(d => d != null);


                foreach (var d in dataSet)
	            {
                    if (d.Cost == 0)
                        clicks++;
                    numExamples++;
	            }
            }

            Console.WriteLine("Clicks: {0} Examples: {1} = {2}", clicks, numExamples, (double)clicks/numExamples);
        }

        static void LearnSplit(string inputDirectory, string prefix, int startHour, int endHour)
        {
            // ${VW} --cb_adf --rank_all --cb_type ips -l 0.1 -f ${modelbase} --interact ud
            var settings = Util.Expand(
                    new[] { "--cb_type ips" },// "--cb_type dr" },
                    new[] { 0.005, 0.005 }.Select(l => string.Format(CultureInfo.InvariantCulture, "-l {0}", l)))
                    .Select(args => new VowpalWabbitSettings("--cb_adf --rank_all --interact ud -b 22 " + args) { EnableExampleCaching = false })
                    .ToList();

            var baseDir = @"c:\temp\hyper\river\";

            for (int i = 0; i < settings.Count; i++)
            {
                var dir = baseDir + i + "\\";
                if (Directory.Exists(dir))
                    Directory.Delete(dir, true);
                Directory.CreateDirectory(dir);
                File.WriteAllText(dir + @"arguments.txt", settings[i].Arguments);
                File.WriteAllText(dir + @"loss.txt", "Hour\t# Examples\t#Features\tLoss\n");
            }

            // overview
            File.WriteAllLines(baseDir + "arguments.txt", settings.Select(s => s.Arguments));

            var dataSet = ReadNonEmpty(inputDirectory, prefix, startHour);

            var positionStats = Enumerable.Range(0, settings.Count).Select(_ => new AutoArray<int>()).ToArray();
            var perfStats = Enumerable.Range(0, settings.Count).Select(_ => new List<VowpalWabbitPerformanceStatistics>()).ToArray();

            using (var vws = new VowpalWabbitSweep<UserContext<DocumentFeatures>, DocumentFeatures>(settings))
            {
                var partitioner = vws.CreatePartitioner();

                for (int hour = startHour; hour < endHour; hour++)
                {
                    Console.WriteLine("LEARN Hour " + hour);
                    {
                        Parallel.ForEach(
                                partitioner,
                                new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount },
                                range =>
                                {
                                    var i = 0;
                                    foreach (var data in dataSet)
                                    {
                                        if (i % settings.Count == range.Item1)
                                        {
                                            var d = (UserContext<DocumentFeatures>)data.Context;
                                            vws.Learn(range.Item1, range.Item2, d, d.ActionDependentFeatures, data.Action - 1, CreateLabel(data));
                                        }

                                        i++;
                                    }
                                });

                        // save all models
                        for (int i = 0; i < vws.VowpalWabbits.Length; i++)
                            vws.VowpalWabbits[i].SaveModel(string.Format(@"{0}\{1}\model_{2:D2}", baseDir, i, hour));
                    }

                    Console.WriteLine("TEST");
                    {
                        var testSettings = Enumerable.Range(0, settings.Count)
                            .Select(i => new VowpalWabbitSettings(string.Format(@"-t -i {0}\{1}\model_{2:D2}", baseDir, i, hour)) { EnableExampleCaching = false })
                            .ToList();

                        using (var vwtest = new VowpalWabbitSweep<UserContext<DocumentFeatures>, DocumentFeatures>(testSettings))
                        {
                            dataSet = ReadNonEmpty(inputDirectory, prefix, hour + 1);

                            Parallel.ForEach(
                                partitioner,
                                new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount },
                                range =>
                                {
                                    foreach (var data in dataSet)
                                    {
                                        var d = (UserContext<DocumentFeatures>)data.Context;
                                        var prediction = vwtest.Predict(range.Item1, range.Item2, d, d.ActionDependentFeatures, data.Action - 1, CreateLabel(data));

                                        // update prediction stats
                                        for (int i = 0; i < prediction.Length; i++)
                                        {
                                            var predIndex = d.ActionDependentFeatures.IndexOf(df => df.Id == prediction[i][0].Id);
                                            positionStats[range.Item1 + i][predIndex]++;
                                        }
                                    }
                                });

                            // log loss
                            for (int i = 0; i < settings.Count; i++)
                            {
                                perfStats[i].Add(vwtest.VowpalWabbits[i].PerformanceStatistics);

                                var ps = vwtest.VowpalWabbits[i].PerformanceStatistics;
                                File.AppendAllText(baseDir + i + @"\loss.txt",
                                string.Format("{0,2} -> {1,2}: {2,7} {3,13} {4} {5}\n",
                                    hour,
                                    hour + 1,
                                    ps.NumberOfExamplesPerPass,
                                    ps.TotalNumberOfFeatures,
                                    ps.AverageLoss,
                                    dataSet.Count));
                            }
                        }
                    }
                }
            }

            for (int i = 0; i < settings.Count; i++)
            {
                File.WriteAllLines(baseDir + i + @"\pred.txt",
                    positionStats[i].ToArray().Select((v, j) => string.Format("{0,8}:{1}", v, j)));

                File.WriteAllLines(baseDir + i + @"\stats.txt",
                     perfStats[i].Select(p => string.Format("{0} {1} {2}",
                         p.NumberOfExamplesPerPass,
                         p.TotalNumberOfFeatures,
                         p.AverageLoss)));
            }

            File.WriteAllLines(baseDir + @"averageLoss.txt",
                    settings.Select((args, i) =>
                        string.Format(
                        "{0}: {1} {2}",
                        args.Arguments,
                        perfStats[i].Sum(p => p.NumberOfExamplesPerPass * p.AverageLoss) / perfStats[i].Sum(p => (double)p.NumberOfExamplesPerPass),
                        perfStats[i].Sum(p => (double)p.NumberOfExamplesPerPass))));
        }

        static void ConvertToString(string inputDirectory, string prefix, DateTime? startTimeInclusive = null, DateTime? endTimeExclusive = null)
        {
            using (var vw = new VowpalWabbit(new VowpalWabbitSettings("--cb_adf --rank_all ") { EnableExampleCaching = false, EnableStringExampleGeneration = true }))
            using (var writer = new StreamWriter(new GZipStream(File.OpenWrite(string.Format("D:\\Data\\{0}.header.{1:yyyyMMdd}-new.vw.gz", prefix, startTimeInclusive)), CompressionLevel.Optimal)))
            {
                var serializer = VowpalWabbitSerializerFactory.CreateSerializer<UserContext<DocumentFeatures>>(vw.Settings).Create(vw);
                var serializerAdf = VowpalWabbitSerializerFactory.CreateSerializer<DocumentFeatures>(vw.Settings).Create(vw);

                var dataSet = Data.Read(inputDirectory, prefix, startTimeInclusive, endTimeExclusive).Where(d => d != null);
                float eps_total = 0f;
                int transfer_tot = 0;
                string modelId = null;

                foreach (var data in dataSet)
                {
                    var d = (UserContext<DocumentFeatures>)data.Context;

                    if (modelId != d.ModelId)
                        Console.WriteLine(d.ModelId);

                    modelId = d.ModelId;

                    uint numActions = (uint)d.ActionDependentFeatures.Count;

                    // t ... exploit
                    // e ... explore
                    char type;

                    // Johns method
                    if (data.Probability < 0.5)
                        type = 'e';
                    else
                    {
                        float l_epsilon = data.Probability - 0.5f;
                        eps_total += l_epsilon / data.Probability;
                        if (Math.Floor(eps_total) > transfer_tot)
                        {
                            type = 'e';
                            transfer_tot++;
                        }
                        else
                        {
                            type = 't';
                        }
                    }

                    //if (type == 'e')
                    //{
                    //    var str = VowpalWabbitMultiLine.SerializeToString<UserContext<DocumentFeatures>, DocumentFeatures>(vw, serializer, serializerAdf, d, d.ActionDependentFeatures, data.Action - 1, CreateLabel(data));
                    //    writer.WriteLine(str);
                    //}
                }
            }
        }

        static void LearnAndTestEditiorialPosition(string inputDirectory, string prefix, int startHour, int endHour)
        {
            var vwArgs = "--cb_adf --rank_all --cb_type ips --noconstant";
            using (var vw = new VowpalWabbit<UserContext<DocumentFeatures>, DocumentFeatures>(new VowpalWabbitSettings(vwArgs) { EnableExampleCaching = false }))
            {
                var sumWeightedCost = new AutoArray<double>();
                var clicks = new AutoArray<int>();
                var actionChosenToDisplay = new AutoArray<int>();
                var sumProb = new AutoArray<double>();
                var sumInvProb = new AutoArray<double>();
                var actionChosenExplore = new AutoArray<int>();
                var actionChosenExploit = new AutoArray<int>();
                var clickExplore = new AutoArray<int>();
                var count = 0;

                for (int hour = startHour; hour <= endHour; hour++)
                {
                    var dataSet = ReadNonEmpty(inputDirectory, prefix, hour);
                    foreach (var data in dataSet)
                    {
                        //var d = (UserContext<DocumentFeatures>)data.Context;
                        //vw.Learn(d, d.ActionDependentFeatures, data.Action - 1, CreateLabel(data));

                        sumWeightedCost[data.Action - 1] += data.Cost * (1.0 / data.Probability);
                        sumProb[data.Action - 1] += data.Probability;
                        sumInvProb[data.Action - 1] += 1.0 / data.Probability;
                        // sum of 1/prob
                        // sum of prob
                        // split per action

                        if (data.Probability > 0.95)
                            actionChosenExploit[data.Action - 1]++;
                        else
                            actionChosenExplore[data.Action - 1]++;

                        actionChosenToDisplay[data.Action - 1]++;

                        if (data.Cost == -1)
                        {
                            //Console.WriteLine("Action {0,2}: {1}: {2,5} {3,5}", data.Action - 1, data.Cost, data.Probability, 1 / data.Probability);
                            clicks[data.Action - 1]++;

                            if (data.Probability <= 0.95)
                            {
                                clickExplore[data.Action - 1]++;
                            }
                        }

                        count++;
                    }
                }

                //vw.Native.SaveModel(@"C:\Temp\E4Lab\msnnc\model."+startHour);

                Console.WriteLine("Action\tWeighted Cost\tTotal\tExpected\tClicks\tChosen\tChosen Exploit\tChosen Explore");
                var arr = sumWeightedCost.ToArray();
                for (int i = 0; i < arr.Length; i++)
                {
                    Console.WriteLine("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}",
                        i, arr[i], count, arr[i] / count, clicks[i], actionChosenToDisplay[i], actionChosenExploit[i], actionChosenExplore[i]);
                }
            }
        }

        static void LearnAndTestThreadedSweep(string inputDirectory, string prefix, int startHour, int endHour)
        {
            // ${VW} --cb_adf --rank_all --cb_type ips -l 0.1 -f ${modelbase} --interact ud
            var settings = Util.Expand(
                    new[] { "--cb_type ips", "--cb_type dr" },
                     new[] { 0.01, 0.005, 0.001 }.Select(l => string.Format(CultureInfo.InvariantCulture, "-l {0}", l)),
                    //new[] { 0.01, 0.05, 0.1, 0.5, 1.0 }.Select(l => string.Format(CultureInfo.InvariantCulture, "-l {0}", l)),
                    new[] { "", "-b 22" }
                    )
                    //new[] { "", "-q ou" })
                    // new[] { "", "" })
                    .Select(args => new VowpalWabbitSettings("--cb_adf --rank_all --interact ud " + args) { EnableExampleCaching = false })
                    .ToList();

            //var settings = new[] { new VowpalWabbitSettings("--cb_adf --rank_all --interact ud -l 1 --cb_type ips ", enableExampleCaching: false) }.ToList();

            var baseDir = @"c:\temp\hyper\river\document-id-only\";

            for (int i = 0; i < settings.Count; i++)
			{
                var dir = baseDir + i + "\\";
                if (Directory.Exists(dir))
                    Directory.Delete(dir, true);
			    Directory.CreateDirectory(dir);
                File.WriteAllText(dir + @"arguments.txt", settings[i].Arguments);
                File.WriteAllText(dir + @"loss.txt", "Hour\t# Examples\t#Features\tLoss\n");
            }

            // overview
            File.WriteAllLines(baseDir + "arguments.txt", settings.Select(s => s.Arguments));

            var dataSet = ReadNonEmpty(inputDirectory, prefix, startHour);

            var positionStats = Enumerable.Range(0, settings.Count).Select(_ => new AutoArray<int>()).ToArray();
            var perfStats = Enumerable.Range(0, settings.Count).Select(_ => new List<VowpalWabbitPerformanceStatistics>()).ToArray();

            using (var vws = new VowpalWabbitSweep<UserContext<DocumentFeatures>, DocumentFeatures>(settings))
            {
                var partitioner = vws.CreatePartitioner();

                for (int hour = startHour; hour < endHour; hour++)
                {
                    Console.WriteLine("LEARN Hour " + hour);
                    {
                        Parallel.ForEach(
                                partitioner,
                                new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount },
                                range =>
                                {
                                    foreach (var data in dataSet)
                                    {
                                        var d = (UserContext<DocumentFeatures>)data.Context;
                                        vws.Learn(range.Item1, range.Item2, d, d.ActionDependentFeatures, data.Action - 1, CreateLabel(data));
                                    }
                                });

                        // save all models
                        for (int i = 0; i < vws.VowpalWabbits.Length; i++)
                            vws.VowpalWabbits[i].SaveModel(string.Format(@"{0}\{1}\model_{2:D2}", baseDir, i, hour));
                    }

                    Console.WriteLine("TEST");
                    {
                        var testSettings = Enumerable.Range(0, settings.Count)
                            .Select(i => new VowpalWabbitSettings(string.Format(@"-t -i {0}\{1}\model_{2:D2}", baseDir, i, hour)
                                /*, enableStringExampleGeneration: true */)
                            { EnableExampleCaching = false })
                            .ToList();

                        using (var vwtest = new VowpalWabbitSweep<UserContext<DocumentFeatures>, DocumentFeatures>(testSettings))
                        {
                            dataSet = ReadNonEmpty(inputDirectory, prefix, hour + 1);

                            Parallel.ForEach(
                                partitioner,
                                new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount },
                                range =>
                                {
                                    //using (var strOut = new StreamWriter(string.Format(@"{0}\{1}\data-{2}.vw", baseDir, range.Item1, hour)))
                                    {
                                        foreach (var data in dataSet)
                                        {
                                            var d = (UserContext<DocumentFeatures>)data.Context;

                                            //strOut.WriteLine(VowpalWabbitMultiLine.SerializeToString(
                                            //    vwtest.VowpalWabbits[range.Item1],
                                            //    vwtest.serializers[range.Item1],
                                            //    vwtest.actionDependentFeatureSerializers[range.Item1],
                                            //    d,
                                            //    d.ActionDependentFeatures,
                                            //    data.Action - 1,
                                            //    CreateLabel(data)));

                                            var prediction = vwtest.Predict(range.Item1, range.Item2, d, d.ActionDependentFeatures, data.Action - 1, CreateLabel(data));

                                            // update prediction stats
                                            for (int i = 0; i < prediction.Length; i++)
                                            {
                                                var predIndex = d.ActionDependentFeatures.IndexOf(df => df.Id == prediction[i][0].Id);
                                                positionStats[range.Item1 + i][predIndex]++;
                                            }
                                        }
                                    }
                                });

                            // log loss
                            for (int i = 0; i < settings.Count; i++)
			                {
                                var ps = vwtest.VowpalWabbits[i].PerformanceStatistics;
                                perfStats[i].Add(ps);

			                    File.AppendAllText(baseDir + i + @"\loss.txt",
                                string.Format("{0,2} -> {1,2}: {2,7} {3,13} {4} {5}\n",
                                    hour,
                                    hour + 1,
                                    ps.NumberOfExamplesPerPass,
                                    ps.TotalNumberOfFeatures,
                                    ps.AverageLoss,
                                    dataSet.Count));
			                }
                        }
                    }
                }
            }

            for (int i = 0; i < settings.Count; i++)
			{
                File.WriteAllLines(baseDir + i + @"\pred.txt",
                    positionStats[i].ToArray().Select((v, j) => string.Format("{0,8}:{1}", v, j)));

			    File.WriteAllLines(baseDir + i + @"\stats.txt",
                     perfStats[i].Select(p => string.Format("{0} {1} {2}",
                         p.NumberOfExamplesPerPass,
                         p.TotalNumberOfFeatures,
                         p.AverageLoss)));
            }

            //  var averageLoss = data.Sum(d => d.Examples * d.Loss) / data.Sum(d => d.Examples);
            File.WriteAllLines(baseDir +  @"averageLoss.txt",
                    settings.Select((args, i) =>
                        string.Format(
                        "{0}: {1} {2}",
                        args.Arguments,
                        perfStats[i].Sum(p => p.NumberOfExamplesPerPass * p.AverageLoss) / perfStats[i].Sum(p => (double)p.NumberOfExamplesPerPass),
                        perfStats[i].Sum(p => (double)p.NumberOfExamplesPerPass))));
        }

        static void Learn(string inputDirectory, string prefix)
        {
            var vwArgs = "--cb_adf --rank_all --interact ud --cb_type ips";
            var holdout = 0.8;
            var model = string.Format("c:\\temp\\{0}.model", prefix);

            using (var vw = new VowpalWabbit<UserContext<DocumentFeatures>, DocumentFeatures>(new VowpalWabbitSettings(vwArgs) { EnableExampleCaching = false }))
            {
                var rnd = new Random(123);
                int exampleCount = 0;
                int nullCount = 0;
                foreach (var data in Data.Read(inputDirectory, prefix))
                {
                    if (data == null)
                    {
                        nullCount++;
                        continue;
                    }
                    var d = (UserContext<DocumentFeatures>)data.Context;

                    if (rnd.NextDouble() < holdout)
                    {
                        vw.Learn(d, d.ActionDependentFeatures, data.Action - 1, CreateLabel(data));
                    }

                    exampleCount++;
                }
                Console.WriteLine("\nNull count: " + nullCount);

                vw.Native.SaveModel(model);
            }

            Console.WriteLine("\n\nLEARN DONE\n\nEVAL");

            // --cb_adf --rank_all --interact ud
            using (var vw = new VowpalWabbit<UserContext<DocumentFeatures>, DocumentFeatures>(new VowpalWabbitSettings(@"-t -i " + model) { EnableExampleCaching = false }))
            {
                var rnd = new Random(123);
                int exampleCount = 0;
                foreach (var data in Data.Read(inputDirectory, prefix))
                {
                    if (data == null)
                    {
                        continue;
                    }
                    var d = (UserContext<DocumentFeatures>)data.Context;
                    if (rnd.NextDouble() >= holdout)
                    {
                        vw.Predict(d, d.ActionDependentFeatures);
                    }

                    exampleCount++;
                }

                // Console.WriteLine(vw.PerformanceStatistics);
            }
        }

        static void ReproValidate(string inputDirectory, string prefix, string modelPrefix, DateTime startTimeInclusive, DateTime endTimeExclusive)
        {
            var count = 0;
            var missing = 0;
            var foundLater = 0;

            ReplayReader.Read(inputDirectory, prefix, modelPrefix, startTimeInclusive, endTimeExclusive,
                modelFile => Console.WriteLine("Restart: " + modelFile),
                modelFile => Console.WriteLine("Reload"),
                (ts, modelId, data) =>
                {
                    count++;
                    if (count % 10000 == 0)
                    {
                        Console.WriteLine("{0,10} vs {1,10} vs {2,10}", count, missing, foundLater);
                    }
                },
                missing: (id, fileName) =>
                {
                    missing++;
                    if (missing % 10000 == 0)
                    {
                        Console.WriteLine("{0,10} vs {1,10} vs {2,10}", count, missing, foundLater);
                    }
                },
                foundLater: id =>
                {
                    foundLater++;
                    if (foundLater % 10000 == 0)
                    {
                        Console.WriteLine("{0,10} vs {1,10} vs {2,10}", count, missing, foundLater);
                    }
                });
        }

        public class RefComparer : IEqualityComparer<float[]>
        {

            public bool Equals(float[] x, float[] y)
            {
                return object.ReferenceEquals(x, y) || x.SequenceEqual(y);
            }

            public int GetHashCode(float[] obj)
            {
                return (int)(obj[0] * 100 + obj[1] * 100);
            }
        }

        static void ExportE4(string inputDirectory, string prefix, string modelPrefix, DateTime startTimeInclusive, DateTime endTimeExclusive)
        {
            var avroSerializer = AvroSerializer.Create<LabContext>();
            var avroSerializerVec = AvroSerializer.Create<Vector>();

            var dict = new Dictionary<float[], Guid>(new RefComparer());
            var docDict = new Dictionary<string, List<Guid>>();
            var mismatch = 0;
            var allData = new List<LabContext>();
            using (var outFile = File.Open(@"C:\Temp\E4Lab\data.avro", FileMode.Create))
            using (var w = AvroContainer.CreateWriter<LabContext>(outFile, Codec.Deflate))
            using (var writer = new SequentialWriter<LabContext>(w, 1024))
            {
            ReplayReader.Read(inputDirectory, prefix, modelPrefix, startTimeInclusive, endTimeExclusive,
                restart: modelFile => { },
                reload: modelFile => { },
                dataFunc: (ts, modelId, data) =>
                {
                    var d = (UserContext<DocumentFeatures>)data.Context;

                    var o = new LabContext
                    {
                        Label = new Label
                        {
                            Action = data.Action,
                            Cost = data.Cost,
                            Probability = data.Probability
                        },
                        Age = d.User.PassportAge,
                        Location = d.User.Location,
                        PageViews = d.User.PrimePageViews,
                        UserLDA = d.UserLDAVector.Vectors,
                            AgeGroup = (Age?)d.User.Age,
                            Gender = (Gender?)d.User.Gender,
                        Documents = d.ActionDependentFeatures.Select(adf => new Document
                        {
                            Id = adf.Id,
                            DocumentLDA = adf.LDAVector.Vectors,
                            Source = adf.Source
                        }).ToArray()
                    };

                    allData.Add(o);

                        //foreach (var vec in o.Documents)
                        //{
                        //    Guid ldaGuid;
                        //    if (dict.TryGetValue(vec.DocumentLDA, out ldaGuid))
                        //    {
                        //        vec.DocumentLDAId = ldaGuid;
                        //    }
                        //    else
                        //    {
                        //        vec.DocumentLDAId = Guid.NewGuid();
                        //        dict.Add(vec.DocumentLDA, vec.DocumentLDAId);
                        //    }

                        //    //List<Guid> assignedGuids;
                        //    //if (docDict.TryGetValue(vec.Id, out assignedGuids))
                        //    //{
                        //    //    if (!assignedGuids.Contains(vec.DocumentLDAId))
                        //    //    {
                        //    //        assignedGuids.Add(vec.DocumentLDAId);
                        //    //    }
                        //    //}
                        //    //else
                        //    //{
                        //    //    docDict.Add(vec.Id, new List<Guid> { vec.DocumentLDAId });
                        //    //}
                        //}

                        writer.Write(o);
                        //avroSerializer.Serialize(outFile, o);
                });
            }
            //foreach (var kvp in docDict)
            //{
            //    Console.WriteLine("{0,20}: {1}", kvp.Key, kvp.Value.Count);
            //}

            // TODO: shuffle
            //using (var outFile = File.OpenWrite(@"C:\Temp\E4Lab\data.avro"))
            //{
            //    foreach (var o in allData)
            //    {
            //        avroSerializer.Serialize(outFile, o);
            //    }
            //}

            using (var outFile = File.OpenWrite(@"C:\Temp\E4Lab\reference.avro"))
            {
                foreach (var kvp in dict)
                {
                    avroSerializerVec.Serialize(outFile, new Vector { ID = kvp.Value, Data = kvp.Key });
                }
            }
        }

        static void LearnReproHyper(string inputDirectory, string prefix, string modelPrefix, DateTime startTimeInclusive, DateTime endTimeExclusive)
        {
            var settings = Util.Expand(
                     new[] { "--cb_type ips", "--cb_type dr" },
                     new[] { 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0 }.Select(l => string.Format(CultureInfo.InvariantCulture, "-l {0}", l))
                    )
                    .Select(args => new VowpalWabbitSettings("--cb_adf --rank_all --interact ud " + args) { EnableExampleCaching = false })
                    .ToList();

            var modelDir = string.Format(@"{0}\{1}-{2:yyyyMMdd}", inputDirectory, modelPrefix, startTimeInclusive);
            var reproModelDir = modelDir + "-hyper";
            Directory.CreateDirectory(reproModelDir);

            string lossFile = reproModelDir + "\\loss.csv";
            if (File.Exists(lossFile))
                File.Delete(lossFile);
            File.AppendAllText(lossFile, "ts.model\tts.data\texamples\tfeatures\tloss\n");

            var testTasks = new List<Task>();

            using (var missingWriter = new StreamWriter(File.Create(Path.Combine(reproModelDir, "missing.log"))))
            using (var vws = new VowpalWabbitSweep<UserContext<DocumentFeatures>, DocumentFeatures>(settings))
            {
                string previousModel = null;
                DateTime previousTimestamp = default(DateTime);

                var trainingSet = new List<TrainingData>();

                ReplayReader.Read(inputDirectory, prefix, modelPrefix, startTimeInclusive, endTimeExclusive,
                    restart: modelFile => {},
                    reload: modelFile => { },
                    dataFunc: (ts, modelId, data) =>
                    {
                        var d = (UserContext<DocumentFeatures>)data.Context;
                        vws.Learn(0, settings.Count, d, d.ActionDependentFeatures, data.Action - 1, CreateLabel(data));

                        trainingSet.Add(data);
                    },
                    missing: (missingEventId, fileName) =>
                    {
                        missingWriter.WriteLine(missingEventId);
                    },
                    endOfTrackback: (ts, startingModelFileName, modelFilename) =>
                    {
                        if (previousModel != null)
                        {
                            var localSet = trainingSet;
                            var localPreviousTimestamp = previousTimestamp;
                            var localPreviousModel = previousModel;

                            var testSettings = Enumerable.Range(0, settings.Count)
                                .Select(i => new VowpalWabbitSettings(string.Format("-t -i {0}.{1}", localPreviousModel, i)) { EnableExampleCaching = false })
                                .ToList();

                            testTasks.Add(Task.Factory.StartNew(() => {
                                using (var vwtest = new VowpalWabbitSweep<UserContext<DocumentFeatures>, DocumentFeatures>(testSettings))
                                {
                                    foreach (var data in localSet)
	                                {
                                        var d = (UserContext<DocumentFeatures>)data.Context;
                                        vwtest.Learn(0, settings.Count, d, d.ActionDependentFeatures, data.Action - 1, CreateLabel(data));
	                                }

                                    lock (lossFile)
                                    {
                                        for (int i = 0; i < settings.Count; i++)
			                            {
                                            File.AppendAllText(lossFile,
                                                string.Format("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\n",
                                                    localPreviousTimestamp,
                                                    ts,
                                                    vwtest.VowpalWabbits[i].PerformanceStatistics.NumberOfExamplesPerPass,
                                                    vwtest.VowpalWabbits[i].PerformanceStatistics.TotalNumberOfFeatures,
                                                    vwtest.VowpalWabbits[i].PerformanceStatistics.AverageLoss,
                                                    localPreviousModel,
                                                    settings[i].Arguments));
			                            }
                                    }
                                }
                            }));
                        }

                        previousModel = reproModelDir + "\\" + Path.GetFileName(modelFilename);
                        for (int i = 0; i < settings.Count; i++)
			            {
                            vws.VowpalWabbits[i].SaveModel(string.Format("{0}.{1}", previousModel, i));
                        }
                        previousTimestamp = ts;

                        trainingSet = new List<TrainingData>();
                    });
            }

            Console.WriteLine("Wait for testing...");
            Task.WaitAll(testTasks.ToArray());
        }


        static void LearnReproModelAlternate(string inputDirectory, string prefix, string modelPrefix, DateTime startTimeInclusive, DateTime endTimeExclusive)
        {
            var modelDir = string.Format(@"{0}\{1}-{2:yyyyMMdd}", inputDirectory, modelPrefix, startTimeInclusive);
            var reproModelDir = modelDir + "-repro";
            Directory.CreateDirectory(reproModelDir);

            string lossFile = modelDir + ".loss.csv";
            if (File.Exists(lossFile))
                File.Delete(lossFile);
            File.AppendAllText(lossFile, "ts.model\tts.data\texamples\tfeatures\tloss\n");

            VowpalWabbit<UserContext<DocumentFeatures>, DocumentFeatures> vw1 = null;
            VowpalWabbit<UserContext<DocumentFeatures>, DocumentFeatures> vw2 = null;

            var testTasks = new List<Task>();
            string previousModel = null;
            DateTime previousTimestamp = default(DateTime);

            var count = 0;
            var trainingSet1 = new List<TrainingData>();
            var trainingSet2 = new List<TrainingData>();

            ReplayReader.Read(inputDirectory, prefix, modelPrefix, startTimeInclusive, endTimeExclusive,
                restart: modelFile =>
                {
                    vw1 = new VowpalWabbit<UserContext<DocumentFeatures>, DocumentFeatures>(new VowpalWabbitSettings("-l 0.005 -i" + modelFile) { EnableExampleCaching = false });
                    vw2 = new VowpalWabbit<UserContext<DocumentFeatures>, DocumentFeatures>(new VowpalWabbitSettings("-l 0.005 -i" + modelFile) { EnableExampleCaching = false });
                },
                reload: modelFile =>
                {
                    if (vw1 != null)
                        vw1.Native.Reload();

                    if (vw2 != null)
                        vw2.Native.Reload();
                },
                dataFunc: (ts, modelId, data) =>
                {
                    var d = (UserContext<DocumentFeatures>)data.Context;
                    if (count % 2 == 0)
                    {
                        vw1.Learn(d, d.ActionDependentFeatures, data.Action - 1, CreateLabel(data));
                        trainingSet1.Add(data);
                    }
                    else
                    {
                        vw2.Learn(d, d.ActionDependentFeatures, data.Action - 1, CreateLabel(data));
                        trainingSet2.Add(data);
                    }

                    count++;
                },
                endOfTrackback: (ts, startingModelFileName, modelFilename) =>
                {
                    if (vw1 != null)
                    {
                        if (previousModel != null)
                        {
                            Console.WriteLine("****************** TEST " + ts);

                            var localSet1 = trainingSet1;
                            var localSet2 = trainingSet2;
                            var localPreviousModel = previousModel;
                            var localPreviousTimestamp = previousTimestamp;

                            testTasks.Add(Task.Factory.StartNew(() => PredictAndReportLoss(localPreviousModel + ".1", localSet1, lossFile, localPreviousTimestamp, ts)));
                            testTasks.Add(Task.Factory.StartNew(() => PredictAndReportLoss(localPreviousModel + ".2", localSet2, lossFile, localPreviousTimestamp, ts)));
                            //PredictAndReportLoss(localPreviousModel + ".1", localSet1, lossFile, localPreviousTimestamp, ts);
                            //PredictAndReportLoss(localPreviousModel + ".2", localSet2, lossFile, localPreviousTimestamp, ts);
                        }


                        using (var vwOnline = new VowpalWabbit("-i " + modelFilename))
                        {
                            // re-use ID to enable comparison
                            vw1.Native.ID = vwOnline.ID;
                            previousModel = reproModelDir + "\\" + Path.GetFileName(modelFilename);
                            vw1.Native.SaveModel(previousModel + ".1");
                            vw2.Native.SaveModel(previousModel + ".2");

                            previousTimestamp = ts;
                        }
                    }

                    trainingSet1 = new List<TrainingData>();
                    trainingSet2 = new List<TrainingData>();
                });

            if (vw1 != null)
            {
                vw1.Dispose();
                vw2.Dispose();
            }

            Console.WriteLine("Wait for testing...");
            Task.WaitAll(testTasks.ToArray());
        }

        static void PredictAndReportLoss(string modelFile, IEnumerable<TrainingData> trainingSet, string lossFile, DateTime previousTimestamp, DateTime timestamp)
        {
            // test previous model on current data
            using (var vwPrevious = new VowpalWabbit<UserContext<DocumentFeatures>, DocumentFeatures>(
                new VowpalWabbitSettings("-t -i" + modelFile) { EnableExampleCaching = false }))
            {
                foreach (var data in trainingSet)
                {
                    var d = (UserContext<DocumentFeatures>)data.Context;
                    vwPrevious.Predict(d, d.ActionDependentFeatures, data.Action - 1, CreateLabel(data));
                }

                lock (lossFile)
                {
                    File.AppendAllText(lossFile,
                        string.Format("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\n",
                            previousTimestamp,
                            timestamp,
                            vwPrevious.Native.PerformanceStatistics.NumberOfExamplesPerPass,
                            vwPrevious.Native.PerformanceStatistics.TotalNumberOfFeatures,
                            vwPrevious.Native.PerformanceStatistics.AverageLoss,
                            modelFile));
                }
            }
        }

        static void LearnReproModel(string inputDirectory, string prefix, string modelPrefix, DateTime startTimeInclusive, DateTime endTimeExclusive)
        {
            var modelDir = string.Format(@"{0}\{1}-{2:yyyyMMdd}", inputDirectory, modelPrefix, startTimeInclusive);
            var reproModelDir = modelDir + "-q";
            Directory.CreateDirectory(reproModelDir);

            string lossFile = modelDir + ".loss.csv";
            if (File.Exists(lossFile))
                File.Delete(lossFile);
            File.AppendAllText(lossFile, "ts.model\tts.data\texamples\tfeatures\tloss\n");

            VowpalWabbit<UserContext<DocumentFeatures>, DocumentFeatures> vw1 =
                new VowpalWabbit<UserContext<DocumentFeatures>, DocumentFeatures>(
                    new VowpalWabbitSettings("--cb_adf --rank_all --noconstant -q sp -q si -l 0.005") { EnableExampleCaching = false });

            var testTasks = new List<Task>();
            string previousModel = null;
            DateTime previousTimestamp = default(DateTime);

            var count = 0;
            var trainingSet1 = new List<TrainingData>();

            ReplayReader.Read(inputDirectory, prefix, modelPrefix, startTimeInclusive, endTimeExclusive,
                restart: modelFile =>
                {
                },
                reload: _ =>
                {
                    if (vw1 != null)
                        vw1.Native.Reload();
                },
                dataFunc: (ts, modelId, data) =>
                {
                    var d = (UserContext<DocumentFeatures>)data.Context;
                    for (int i = 0; i < d.ActionDependentFeatures.Count; i++)
                        d.ActionDependentFeatures[i].EditorialPosition = i;

                    vw1.Learn(d, d.ActionDependentFeatures, data.Action - 1, CreateLabel(data));
                    trainingSet1.Add(data);

                    count++;
                },
                endOfTrackback: (ts, modelFilename, _) =>
                {
                    if (vw1 != null)
                    {
                        if (previousModel != null)
                        {
                            Console.WriteLine("****************** TEST " + ts);

                            var localSet1 = trainingSet1;
                            var localPreviousModel = previousModel;
                            var localPreviousTimestamp = previousTimestamp;

                            testTasks.Add(Task.Factory.StartNew(() => PredictAndReportLoss(localPreviousModel + ".1", localSet1, lossFile, localPreviousTimestamp, ts)));
                        }

                        using (var vwOnline = new VowpalWabbit("-i " + modelFilename))
                        {
                            // re-use ID to enable comparison
                            vw1.Native.ID = vwOnline.ID;
                            previousModel = reproModelDir + "\\" + Path.GetFileName(modelFilename);
                            vw1.Native.SaveModel(previousModel + ".1");

                            previousTimestamp = ts;
                        }
                    }

                    trainingSet1 = new List<TrainingData>();
                });

            if (vw1 != null)
            {
                vw1.Dispose();
            }

            Console.WriteLine("Wait for testing...");
            Task.WaitAll(testTasks.ToArray());
        }


        static void EvalReproModel(string inputDirectory, string prefix, string modelPrefix, DateTime startTimeInclusive, DateTime endTimeExclusive)
        {
            var modelDir = string.Format(@"{0}\{1}-{2:yyyyMMdd}", inputDirectory, modelPrefix, startTimeInclusive);

            Console.WriteLine("Reading trackback from {0}", modelDir);
            var trackbackQuery =
                from f in Directory.EnumerateFiles(modelDir)
                let match = Regex.Match(Path.GetFileName(f), @"^model-(\d+)-(\d+)(.trackback(?:.deployed)?)?$")
                where match.Success
                let output = new
                {
                    Filename = f,
                    Timestamp = new DateTime(long.Parse(match.Groups[2].Value)),
                    IsTrackback = match.Groups[3].Success,
                    IsDeployed = match.Groups[3].Value.EndsWith("deployed")
                }
                group output by output.Timestamp into g
                orderby g.Key
                let output = new
                {
                    Timestamp = g.Key,
                    Model = g.FirstOrDefault(a => !a.IsTrackback),
                    Trackback = g.FirstOrDefault(a => a.IsTrackback),
                    Files = g.ToArray()
                }
                where output.Model != null && output.Trackback != null
                select output;

            var trackback = trackbackQuery.ToList();

            foreach (var block in trackback)
            {
                if (block.Files.Length != 2)
                {
                    Console.WriteLine("Invalid block: {0}", string.Join(",", block.Files.Select(f => f.Filename)));
                }
            }

            var vws = new Dictionary<string, Tuple<VowpalWabbit<UserContext<DocumentFeatures>, DocumentFeatures>, ActionBlock<TrainingData>>>();

            var modelToIDs = new Dictionary<string, string[]>();

            string previousModel = null;
            foreach (var block in trackback)
	        {
                var trackbackData = File.ReadAllLines(block.Trackback.Filename);

                var modelId = trackbackData[0];
                if (!modelId.StartsWith("model-"))
                {
                    Console.WriteLine("special trackback block");
                    continue;
                }

                modelToIDs.Add(modelId, trackbackData.Skip(1).ToArray());
                var vw = new VowpalWabbit<UserContext<DocumentFeatures>, DocumentFeatures>(new VowpalWabbitSettings("-t -i " + block.Model.Filename) { EnableExampleCaching = false });
                vws.Add(vw.Native.ID,
                    Tuple.Create(vw, new ActionBlock<TrainingData>(
                        data =>
                            {
                                var d = (UserContext<DocumentFeatures>)data.Context;
                                vw.Predict(d, d.ActionDependentFeatures, data.Action - 1, CreateLabel(data));
                            },
                        new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 1 })));

                var count = 0;
                var missing = 0;

                //train data on same model (seed from previous file)
                //if (previousModel != null)
                //{
                //    //using (var vw = new VowpalWabbit<UserContext<DocumentFeatures>, DocumentFeatures>("--cb_adf --rank_all --interact ud --cb_type dr -l 0.005 -i " + previousModel))
                //    using (var vw = new VowpalWabbit<UserContext<DocumentFeatures>, DocumentFeatures>(new VowpalWabbitSettings("-t -i " + block.Model.Filename, enableExampleCaching: false)))
                //    {
                //        foreach (var eventId in trackbackData.Skip(1))
                //        {
                //            TrainingData data;
                //            if (!dataset.TryGetValue(eventId, out data))
                //            {
                //                missing++;
                //                continue;
                //            }

                //            var d = (UserContext<DocumentFeatures>)data.Context;

                //            vw.Predict(d, d.ActionDependentFeatures, data.Action - 1, CreateLabel(data));

                //            count++;
                //        }

                //        Console.WriteLine("Average loss: {0}", vw.Native.PerformanceStatistics.AverageLoss);
                //    }
                //}

                previousModel = block.Model.Filename;
                Console.WriteLine("{0}: valid: {1} missing: {2}", block.Timestamp, count, missing);
	        }

            foreach (var data in Data.Read(inputDirectory, prefix, startTimeInclusive, startTimeInclusive.AddDays(1)))
            {
                var d = (UserContext<DocumentFeatures>)data.Context;
                vws[d.ModelId].Item2.Post(data);
            }

            foreach (var kvp in vws)
            {
                kvp.Value.Item2.Complete();
                kvp.Value.Item2.Completion.Wait();

                Console.WriteLine("Model {0} Loss: {1} Examples: {2} Features: {3}", kvp.Key,
                    kvp.Value.Item1.Native.PerformanceStatistics.AverageLoss,
                    kvp.Value.Item1.Native.PerformanceStatistics.NumberOfExamplesPerPass,
                    kvp.Value.Item1.Native.PerformanceStatistics.TotalNumberOfFeatures);
            }

            //Console.WriteLine("Reading data");
            //var dataset = Data.Read(inputDirectory, prefix, startTimeInclusive, startTimeInclusive.AddDays(1))
            //    //.Take(1000)
            //    .ToDictionary(t => t.Id);
        }
    }
}
