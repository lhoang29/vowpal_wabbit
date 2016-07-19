using Microsoft.Content.Recommendations.TrainingRuntime.Context;
using Microsoft.Content.Recommendations.TrainingRuntime.Trainer;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using VW;

namespace msn
{
    public class ReplayTrainer
    {
        public static void Run(string inputDirectory, string prefix, string modelPrefix, DateTime startTimeInclusive, DateTime endTimeExclusive)
        {
            var modelDir = string.Format(@"{0}\{1}-{2:yyyyMMdd}", inputDirectory, modelPrefix, startTimeInclusive);
            var reproModelDir = modelDir + "-replay";
            Directory.CreateDirectory(reproModelDir);

            string missingFile = Path.Combine(reproModelDir, "missing.log");
            string mismatchFile = Path.Combine(reproModelDir, "mismatch.log");
            if (File.Exists(missingFile))
            {
                File.Delete(missingFile);
            }
            if (File.Exists(mismatchFile))
            {
                File.Delete(mismatchFile);
            }

            VowpalWabbit<UserContext<DocumentFeatures>, DocumentFeatures> vw = null;
            var trainingSet = new List<TrainingData>();

            ReplayReader.Read(inputDirectory, prefix, modelPrefix, startTimeInclusive, endTimeExclusive,
                readAheadMax: 500000,
                restart: modelFile =>
                {
                    if (vw != null)
                    {
                        vw.Dispose();
                    }
                    vw = new VowpalWabbit<UserContext<DocumentFeatures>, DocumentFeatures>(
                            new VowpalWabbitSettings("--quiet -l 0.005 -i " + modelFile) { EnableExampleCaching = false });
                },
                reload: modelFile => 
                {
                    // Reload only works if no events are missing from the last point a model was restarted.
                    // If anything is missing, model is set to null.
                    if (vw != null)
                    {
                        foreach (TrainingData data in trainingSet)
                        {
                            var d = (UserContext<DocumentFeatures>)data.Context;
                            vw.Learn(d, d.ActionDependentFeatures, data.Action - 1, Program.CreateLabel(data));
                        }
                        vw.Native.Reload();
                        trainingSet.Clear();
                    }
                },
                dataFunc: (ts, modelId, data) =>
                {
                    trainingSet.Add(data);
                },
                missing: (missingEventId, fileName) =>
                {
                    // events were missing, reset model since it will no longer match
                    if (vw != null)
                    {
                        vw.Dispose();
                        vw = null;
                    }
                    // also reset training data stack
                    trainingSet.Clear();

                    File.AppendAllText(missingFile, missingEventId + " " + fileName + "\r\n");
                },
                endOfTrackback: (ts, startingModelFileName, modelFilename) =>
                {
                    // model is not null only if a restart or reload was encountered, otherwise not reproducible
                    if (vw != null)
                    {
                        foreach (TrainingData data in trainingSet)
                        {
                            var d = (UserContext<DocumentFeatures>)data.Context;
                            vw.Learn(d, d.ActionDependentFeatures, data.Action - 1, Program.CreateLabel(data));
                        }

                        using (var vwOriginal = new VowpalWabbit<UserContext<DocumentFeatures>, DocumentFeatures>(
                            new VowpalWabbitSettings("--quiet -i " + modelFilename)))
                        {
                            // Copy ID to make models identical
                            vw.Native.ID = vwOriginal.Native.ID;
                        }

                        string outputModel = Path.Combine(reproModelDir, Path.GetFileName(modelFilename));
                        vw.Native.SaveModel(outputModel);

                        // Check for mismatch
                        if (!File.ReadAllBytes(outputModel).SequenceEqual(File.ReadAllBytes(modelFilename)))
                        {
                            File.AppendAllText(mismatchFile, string.Format("Model: {0}, Begin Model: {1}, # Events Trained: {2}\r\n",
                                Path.GetFileName(modelFilename),
                                Path.GetFileName(startingModelFileName),
                                trainingSet.Count));
                        }
                    }
                    
                    trainingSet.Clear();
                }
            );

            if (vw != null)
            {
                vw.Dispose();
            }
        }

        public static void BinaryCompareMatchingFiles(string dir1, string dir2)
        {
            string[] files1 = Directory.GetFiles(dir1);
            string[] files2 = Directory.GetFiles(dir2);

            foreach (string f1 in files1)
            {
                string f2 = Path.Combine(Path.GetDirectoryName(dir2), Path.GetFileName(f1));
                if (File.Exists(f2))
                {
                    if (!File.ReadAllBytes(f1).SequenceEqual(File.ReadAllBytes(f2)))
                    {
                        Console.WriteLine("File content not match: {0}", Path.GetFileName(f1));
                    }
                }
            }
        }
    }
}
