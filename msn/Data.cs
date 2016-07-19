using Microsoft.Content.Recommendations.TrainingRuntime.Context;
using Microsoft.Content.Recommendations.TrainingRuntime.ContextLocal;
using Microsoft.Content.Recommendations.TrainingRuntime.Reader;
using Microsoft.Content.Recommendations.TrainingRuntime.RewardFunction;
using Microsoft.Content.Recommendations.TrainingRuntime.Trainer;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace msn
{
    public static class Data
    {
        public static IEnumerable<TrainingData> Read(string inputDirectory, string localPrefix, DateTime? startTimeInclusive = null, DateTime? endTimeExclusive = null)
        {
            //var localStart = startInclusive;
            //var localEnd = endExclusive;
            //var localPrefix = myPrefix ?? prefix;
            //Console.WriteLine(string.Format(CultureInfo.InvariantCulture, "Reading {0} to {1}", localStart, localEnd));
            Console.WriteLine("\nReading {0}\n", localPrefix);

            int capacity = 200000;
            var fdr = new FileDataReader<UserContext<DocumentFeatures>, DocumentFeatures>(inputDirectory, localPrefix, capacity, new ClickRewardFunction(), startTimeInclusive, endTimeExclusive);

            while (fdr.MoveNext())
            {
                // action: [1,2,3]
                // doc: [A,B,C]
                //
                //
                // fdr.GetCurrentTrainingData().
                var td = fdr.GetCurrentTrainingData();
                if (td == null)
                {
                    yield return null;
                    continue;
                }

                var userCtx = td.Context as UserContext<DocumentFeatures>;

                // check whether we need to create dummy user vector
                if (userCtx.UserLDAVector == null || userCtx.UserLDAVector.Vectors == null)
                {
                    var docFeature = userCtx.ActionDependentFeatures.FirstOrDefault(x => x.LDAVector != null && x.LDAVector.Vectors != null);
                    if (docFeature == null)
                    {
                        yield return null;
                        continue;
                    }

                    userCtx.UserLDAVector = new LDAFeatureVector(docFeature.LDAVector.Vectors.Length);
                }

                // dummy document vectors check
                var docVectorLength = userCtx.UserLDAVector.Vectors.Length;

                foreach (var adf in userCtx.ActionDependentFeatures)
                {
                    if (adf.LDAVector == null)
                    {
                        adf.LDAVector = new LDAFeatureVector(docVectorLength);
                    }
                }

                yield return td;
            }
        }

        public static void GenerateFolderStructure(string dataPath, string containerName, int startHour, int endHour)
        {
            List<string> files999 = new List<string>();
            for (int i = startHour; i < endHour; i++)
            {
                string filePrefix = i.ToString().PadLeft(2, '0');

                string hourDir = Path.Combine(dataPath, filePrefix + "-" + containerName);
                if (Directory.Exists(hourDir))
                {
                    continue;
                }

                Directory.CreateDirectory(hourDir);

                string[] files = Directory.GetFiles(dataPath, filePrefix + "*");

                files999.Add(files[files.Length - 1]); // add 999 file

                foreach (string f in files)
                {
                    File.Copy(f, Path.Combine(hourDir, Path.GetFileName(f)), true);
                }

                // copy all 999 files up to this hour
                foreach (string f999 in files999)
                {
                    File.Copy(f999, Path.Combine(hourDir, Path.GetFileName(f999)), true);
                }
            }
        }

        public static void TestFolderStructure(string path)
        {
            string[] dirs = Directory.GetDirectories(path);
            foreach (string dir in dirs)
            {
                Console.WriteLine("Testing {0}", dir);

                string[] files = Directory.GetFiles(dir);

                foreach (string file in files)
                {
                    string originalFile = Path.Combine(path, Path.GetFileName(file));
                    if (!File.Exists(originalFile))
                    {
                        Console.WriteLine("Original file missing {0}", originalFile);
                    }

                    if (!File.ReadAllBytes(file).SequenceEqual(File.ReadAllBytes(originalFile)))
                    {
                        Console.WriteLine("File content not matching, copied: {0}, original: {1}", file, originalFile);
                    }
                }

                // Test 999 files
                int hour = int.Parse(Path.GetFileName(dir).Substring(0, 2));
                for (int h = 0; h <= hour; h++)
                {
                    string[] h999 = Directory.GetFiles(dir, h.ToString("00") + "-999*");
                    if (h999.Length != 1)
                    {
                        Console.WriteLine("Number of 999 files in {0} is {1} for hour {2}, expected exactly 1", dir, h999.Length, h);
                    }
                }
            }
        }

        public static bool IncludeRecord(TrainingData data, Random rand)
        {
            if (data == null)
            {
                return false;
            }
            float q = 0;
            if (data.Probability > 0.95f) // exploitation
            {
                q = data.Probability - 0.95f;

                if (rand.NextDouble() >= q / data.Probability)
                {
                    return false;
                }
                data.Probability = q;
                return true;
            }
            return true;
        }

        public static void Convert(string inputDirectory, string prefix)
        {
            int startHour = 7;
            int endHour = 23;

            using (var fOut = new StreamWriter(
                new GZipStream(
                File.OpenWrite(string.Format(@"c:\lab\{0}.json.gz", prefix)),
                CompressionLevel.Optimal), Encoding.UTF8))
            {
                var jsonSerializer = new JsonSerializer();
                jsonSerializer.ReferenceResolver = new CachingReferenceResolver(100000);
                for (int hour = startHour; hour < endHour; hour++)
                {
                    Console.WriteLine("Convert " + hour);
                    {
                        var rnd = new Random(hour);

                        foreach (var data in Read(inputDirectory, hour.ToString().PadLeft(2, '0') + "-" + prefix))
                        {
                            if (!IncludeRecord(data, rnd))
                            {
                                continue;
                            }
                            var d = (UserContext<DocumentFeatures>)data.Context;

                            var o = new LabContext
                            {
                                Label = new Label
                                {
                                    Action = data.Action,
                                    Cost = data.Cost,
                                    Probability = data.Probability
                                },
                                //AgeGroup = (Age?)d.User.Age,
                                Age = d.User.PassportAge,
                                //Gender = (Gender?)d.User.Gender,
                                Location = d.User.Location,
                                PageViews = d.User.PrimePageViews,
                                UserLDA = d.UserLDAVector.Vectors,
                                Documents = d.ActionDependentFeatures.Select(adf => new Document
                                {
                                    Id = adf.Id,
                                    DocumentLDA = adf.LDAVector.Vectors,
                                    Source = adf.Source
                                }).ToArray()
                            };

                            jsonSerializer.Serialize(fOut, o);
                            fOut.WriteLine();
                        }
                    }
                }
            }
        }

        public static List<Header> ReadHeader(string inputDirectory, string prefix, DateTime startTimeInclusive, DateTime endTimeExclusive)
        {
            return Read(inputDirectory, prefix, startTimeInclusive, endTimeExclusive)
                .Where(d => d != null)
                .Select(d => new Header
                {
                    Action = d.Action,
                    Cost = d.Cost,
                    Probability = d.Probability,
                    ChoosenDocId = ((UserContext<DocumentFeatures>)d.Context).ActionDependentFeatures[d.Action - 1].Id
                })
                .ToList();
        }

        static void WriteCSV(List<Header> header, string prefix)
        {
            using (var f = File.CreateText(string.Format("c:\\temp\\{0}.csv", prefix)))
            {
                f.WriteLine("Action,Cost,Probability,ChoosenDocId");
                foreach (var h in header)
                {
                    f.WriteLine("{0},{1},{2},\"{3}\"", h.Action, h.Cost, h.Probability, h.ChoosenDocId);
                }
            }
        }

        static void ExtractHeader(string inputDirectory, string prefix)
        {
            using (var sw = File.CreateText(@"c:\temp\header"))
            {
                foreach (var d in Read(inputDirectory, prefix))
                {
                    sw.WriteLine("{0}:{1}:{2}", d.Action, d.Cost, d.Probability);
                }
            }
        }
    }
}
