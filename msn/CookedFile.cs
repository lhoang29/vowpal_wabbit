using Microsoft.Content.Recommendations.TrainingRuntime.Context;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace msn
{
    public class CookedFile
    {
        public CookedFile() { }

        public CookedFile(JsonSerializerSettings jsonSettings, float epsilon, string directory)
        {
            this.jsonSettings = jsonSettings;
            this.epsilon = epsilon;
            this.directory = directory;
        }

        public static List<CookedFile> EnumerateDirectory(string directory, float epsilon)
        {
            Console.WriteLine("Reading dictionary...");
            Dictionary<string, DocumentFeatures> documentLDAVectorDict;
            using (var dictReader = new JsonTextReader(new StreamReader(new GZipStream(new FileStream(Path.Combine(directory, "dict.json.gz"), FileMode.Open), CompressionMode.Decompress))))
            {
                documentLDAVectorDict = JsonSerializer.Create().Deserialize<Dictionary<string, DocumentFeatures>>(dictReader);
            }

            var resolver = new OutputCachingReferenceResolver(documentLDAVectorDict.ToDictionary(kv => kv.Key, kv => (object)kv.Value));
            var jsonSettings = new JsonSerializerSettings() { ReferenceResolverProvider = () => resolver };

            return
                (from f in Directory.EnumerateFiles(Path.Combine(directory, "train"))
                 let m = Regex.Match(Path.GetFileName(f), @"^(\d+)-(\d+)-(.+)\.gz$")
                 where m.Success
                 let modelId = m.Groups[3].Value
                 let testSetFile = Path.Combine(directory, "test", modelId + ".gz")
                 select new CookedFile
                 {
                     File = f,
                     Nr = int.Parse(m.Groups[1].Value),
                     ModelId = modelId,
                     jsonSettings = jsonSettings,
                     epsilon = epsilon,
                     directory = directory,
                     TestFile = !System.IO.File.Exists(testSetFile) ? null :
                        new CookedFile
                        {
                            File = testSetFile,
                            jsonSettings = jsonSettings,
                            epsilon = epsilon,
                            ModelId = modelId,
                            directory = directory
                        }
                 })
                 .OrderBy(f => f.Nr)
                 .ToList();
        }

        public static List<CookedFile> EnumerateDirectory(string directory, float epsilon,
            out Dictionary<string, int> ldaToIndex,
            out Dictionary<string, int> jsonKeyToLDAIndex)
        {
            Console.WriteLine("Reading dictionary...");
            Dictionary<string, DocumentFeatures> documentLDAVectorDict;
            using (var dictReader = new JsonTextReader(new StreamReader(new GZipStream(new FileStream(Path.Combine(directory, "dict.json.gz"), FileMode.Open), CompressionMode.Decompress))))
            {
                documentLDAVectorDict = JsonSerializer.Create().Deserialize<Dictionary<string, DocumentFeatures>>(dictReader);
            }

            Console.WriteLine("Building dictionary features ...");
            // Take unique features
            Dictionary<string, List<string>> ldaToJsonKeys = documentLDAVectorDict
                .AsParallel()
                .Select(kvp => new KeyValuePair<string, string>(kvp.Value.GetLDAString(), kvp.Key))
                .GroupBy(kvp => kvp.Key, kvp => kvp.Value)
                .ToDictionary(g => g.Key, g => g.ToList());

            ldaToIndex = new Dictionary<string, int>(); // mapping from each unique lda string to its index
            jsonKeyToLDAIndex = new Dictionary<string, int>(); // mapping from each json key to lda string index
            int index = 0;
            foreach (string ldaString in ldaToJsonKeys.Keys)
            {
                ldaToIndex.Add(ldaString, index);
                foreach (string key in ldaToJsonKeys[ldaString])
                {
                    jsonKeyToLDAIndex.Add(key, index);
                }
                index++;
            }

            var resolver = new OutputCachingReferenceResolver(documentLDAVectorDict.ToDictionary(kv => kv.Key, kv => (object)kv.Value));
            var jsonSettings = new JsonSerializerSettings() { ReferenceResolverProvider = () => resolver };

            return
                (from f in Directory.EnumerateFiles(Path.Combine(directory, "train"))
                 let m = Regex.Match(Path.GetFileName(f), @"^(\d+)-(\d+)-(.+)\.gz$")
                 where m.Success
                 let modelId = m.Groups[3].Value
                 let testSetFile = Path.Combine(directory, "test", modelId + ".gz")
                 select new CookedFile
                 {
                     File = f,
                     Nr = int.Parse(m.Groups[1].Value),
                     ModelId = modelId,
                     jsonSettings = jsonSettings,
                     epsilon = epsilon,
                     directory = directory,
                     TestFile = !System.IO.File.Exists(testSetFile) ? null :
                        new CookedFile
                        {
                            File = testSetFile,
                            jsonSettings = jsonSettings,
                            epsilon = epsilon,
                            ModelId = modelId,
                            directory = directory
                        }
                 })
                 .OrderBy(f => f.Nr)
                 .ToList();
        }

        private string directory;

        private float epsilon;

        private JsonSerializerSettings jsonSettings;

        public string File { get; set; }

        public int Nr { get; set; }

        public string ModelId { get; set; }

        public CookedFile TestFile { get; set; }

        private IEnumerable<string> ReadAllLines()
        {
            int sleepTimeMs = 100;
            int totalSleepTimeMs = 0;
            int maxSleepTimeMs = 30 * 60 * 1000; // 30 minutes
            while (!System.IO.File.Exists(File) && totalSleepTimeMs < maxSleepTimeMs)
            {
                System.Threading.Thread.Sleep(sleepTimeMs);
                totalSleepTimeMs += sleepTimeMs;
            }
            if (totalSleepTimeMs >= maxSleepTimeMs)
            {
                throw new FileNotFoundException("Cannot find specified file, waited for " + totalSleepTimeMs + "ms.", File);
            }

            using (var ms = new MemoryStream(System.IO.File.ReadAllBytes(File)))
            {
                using (var fileReader = new StreamReader(new GZipStream(ms, CompressionMode.Decompress)))
                {
                    string line;
                    while ((line = fileReader.ReadLine()) != null)
                    {
                        yield return line;
                    }
                }
            }
            
        }

        public List<TrainingDataOutput> Deserialize()
        {
            float eps_total = 0f;
            int transfer_tot = 0;

            return ReadAllLines()
                    .AsParallel()
                    .AsOrdered()
                    .WithDegreeOfParallelism(Environment.ProcessorCount / 2)
                    .Select(line => JsonConvert.DeserializeObject<TrainingDataOutput>(line, jsonSettings))
                    .AsSequential()
                    .Select(d =>
                    {
                        // figure if an event belongs to explore or exploit
                        var probability = d.Probability;

                        bool isExplore;
                        if (probability < epsilon)
                            isExplore = true;
                        else
                        {
                            float l_epsilon = probability - 1 + epsilon;
                            eps_total += l_epsilon / probability;
                            if (Math.Floor(eps_total) > transfer_tot)
                            {
                                isExplore = true;
                                transfer_tot++;
                            }
                            else
                            {
                                isExplore = false;
                            }
                        }

                        d.IsExplore = isExplore;

                        return d;
                    })
                    .ToList();
        }
    }
}
