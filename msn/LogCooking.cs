using Microsoft.Content.Recommendations.TrainingRuntime.Context;
using Microsoft.Content.Recommendations.TrainingRuntime.Trainer;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using VW;

namespace msn
{
    /// <summary>
    /// Training data
    /// </summary>
    public class TrainingDataOutput
    {
        /// <summary>
        /// Training data identifier
        /// </summary>
        [JsonProperty(PropertyName = "i", NullValueHandling = NullValueHandling.Ignore)]
        public string Id { get; set; }

        /// <summary>
        /// Top action chosen
        /// </summary>
        [JsonProperty(PropertyName = "a")]
        public int Action { get; set; }

        /// <summary>
        /// Cost of the action
        /// </summary>
        [JsonProperty(PropertyName = "c")]
        public float Cost { get; set; }

        /// <summary>
        /// Probability of selecting the action
        /// </summary>
        [JsonProperty(PropertyName = "p")]
        public float Probability { get; set; }

        /// <summary>
        /// Features to feed into VW
        /// </summary>
        //public string Features { get; set; }

        /// <summary>
        /// Annotated context object to feed into VW
        /// </summary>
        [JsonProperty(PropertyName = "o")]
        public UserContext<DocumentFeatures> Context { get; set; }

        [JsonIgnore]
        public bool IsExplore { get; set; }
    }

    public class DataWriter<T>
    {
        private StreamWriter jsonWriter;

        private OutputCachingReferenceResolver resolver;

        private JsonSerializerSettings serializerSettings;

        private ActionBlock<T> writerBlock;

        public DataWriter(string outputFilename, IReferenceResolver resolver)
        {
            outputFilename += ".gz";
            Console.WriteLine(outputFilename);
            this.jsonWriter = new StreamWriter(new GZipStream(new FileStream(outputFilename, FileMode.Create), CompressionLevel.Optimal));
            // this.resolver = new OutputCachingReferenceResolver();
            this.serializerSettings = new JsonSerializerSettings { ReferenceResolver = resolver };

            //this.writerBlock = new ActionBlock<T>(
            //    data => this.jsonWriter.WriteLine(JsonConvert.SerializeObject(data,Formatting.None, this.serializerSettings)),
            //    new ExecutionDataflowBlockOptions
            //    {
            //        MaxDegreeOfParallelism = 1
            //    });
        }

        public void Write(T data)
        {
            this.jsonWriter.WriteLine(JsonConvert.SerializeObject(data,Formatting.None, this.serializerSettings));

            //this.writerBlock.Post(data);
        }

        public void Close()
        {
            //this.writerBlock.Complete();
            //return this.writerBlock.Completion.ContinueWith(t => jsonWriter.Close());
            this.jsonWriter.Close();
        }
    }

    public class LogCooking
    {
        public static void Run(string inputDirectory, string prefix, string modelPrefix, DateTime startTimeInclusive, DateTime endTimeExclusive)
        {
            var modelDir = string.Format(@"{0}\{1}-{2:yyyyMMdd}", inputDirectory, modelPrefix, startTimeInclusive);
            var reproModelDir = modelDir + "-cooked";
            var testDir = Path.Combine(reproModelDir, "test");
            var trainDir = Path.Combine(reproModelDir, "train");
            Directory.CreateDirectory(reproModelDir);
            Directory.CreateDirectory(testDir);
            Directory.CreateDirectory(trainDir);


            var resolver = new OutputCachingReferenceResolver();
            string lastModelId = null;
            var testSets = new Dictionary<string, DataWriter<TrainingDataOutput>>();
            var trainDataSet = new List<TrainingDataOutput>();

            var closeTasks = new List<Task>();
            int learnModelOrder = 0;

            ReplayReader.Read(inputDirectory, prefix, modelPrefix, startTimeInclusive, endTimeExclusive,
                readAheadMax: 150000,
                restart: _ => { },
                reload: _ => { },
                dataFunc: (ts, modelId, data) =>
                {
                    var trainingDataOutput = new TrainingDataOutput
                    {
                        Action = data.Action,
                        Cost = data.Cost,
                        Probability = data.Probability,
                        Context = (UserContext<DocumentFeatures>)data.Context
                    };

                    trainDataSet.Add(trainingDataOutput);

                    if (trainingDataOutput.Context.ModelId == null)
                        return;

                    DataWriter<TrainingDataOutput> testDataSet;
                    if (!testSets.TryGetValue(trainingDataOutput.Context.ModelId, out testDataSet))
                    {
                        testDataSet = new DataWriter<TrainingDataOutput>(Path.Combine(testDir, trainingDataOutput.Context.ModelId), resolver);
                        testSets.Add(trainingDataOutput.Context.ModelId, testDataSet);
                    }
                    testDataSet.Write(trainingDataOutput);
                },
                missing: (_, __) => { },
                endOfTrackback: (ts, startingModelFileName, modelFilename) =>
                {
                    string modelId;
                    using (var vwOriginal = new VowpalWabbit<UserContext<DocumentFeatures>, DocumentFeatures>(new VowpalWabbitSettings("--quiet -i " + modelFilename)))
                    {
                        // Copy ID to make models identical
                        modelId = vwOriginal.Native.ID;
                    }

                    var outputFilename = string.Format("{0}-{1:yyyyMMddHHmm}-{2}", learnModelOrder, ts, modelId);
                    //Console.WriteLine("Writing {0} examples to {1}-{2} from trackback: {3}", trainDataSet.Count, learnModelOrder, modelId, modelFilename);
                    var dataSet = new DataWriter<TrainingDataOutput>(Path.Combine(trainDir, outputFilename), resolver);
                    foreach (var td in trainDataSet)
                    {
                        dataSet.Write(td);
                    }
                    trainDataSet.Clear();

                    dataSet.Close();
                    //closeTasks.Add(dataSet.Close());
                    learnModelOrder++;
                }
                //, stop: () => learnModelOrder > 50
            );

            foreach (var t in testSets)
                t.Value.Close();

            //closeTasks.AddRange(testSets.Values.Select(t => t.Close()));

            //Console.WriteLine("Waiting for data sets to finish writing...");
            //Task.WaitAll(closeTasks.ToArray());

            using (var jsonWriter = new JsonTextWriter(new StreamWriter(new GZipStream(new FileStream(Path.Combine(reproModelDir, "dict.json.gz"), FileMode.Create), CompressionLevel.Optimal))))
            {
                JsonSerializer.Create().Serialize(jsonWriter, resolver.OutputDictionary);
            }
        }
    }
}
