namespace Microsoft.Content.Recommendations.TrainingRuntime.Test
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    //using Microsoft.Content.Recommendations.CloudTrainingCommonTest;
    //using Microsoft.Content.Recommendations.TrainingRuntime;
    using Microsoft.Content.Recommendations.TrainingRuntime.Context;
    using Microsoft.Content.Recommendations.TrainingRuntime.Trainer;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Serialization;
    using VW;
    using VW.Labels;
    using VW.Serializer;

    public class ModelTest
    {
        private static readonly int DataCount = 30000;

        private static readonly int NumberOfActions = 30;

        private static readonly int NumberOfTopics = 500;

        private static readonly string TrackbackFile = "trackback";

        private static readonly string SavedModelFileVW = "saved_vw.model";

        private static readonly string SavedModelFileTraining = "saved_training.model";

        private static readonly string InitialSavedModelFile = "initial_training.model";

        private static readonly string TrainingParamsFormat = "--cb_adf --cb_type ips --interact ud -q ou -l 1.5 --rank_all -f {0}";

        //private static readonly Dictionary<string, string> TrainerOptionStd = new Dictionary<string, string>();

        /*private static readonly Dictionary<string, string> TrainerOptionInteract = new Dictionary<string, string>()
        {
            { TrainerOptions.Interact, "ud" },
            { TrainerOptions.Custom, "--cb_type ips -q ou -l 1.5" },
            { TrainerOptions.CustomResume, "-l 1.5" },
        };*/


        public static bool Run()
        {
            bool testPassed = true;
            VowpalWabbit<UserContext<DocumentFeatures>, DocumentFeatures> vw = null;
            //ITrainer trainer = null;

            try
            {
                List<TrainingData> trainingData = new List<TrainingData>();
                
                var dataGen = new DataGeneration();
                for (var i = 0; i < DataCount; i++)
                {
                    var context = dataGen.GenerateRandomContext(NumberOfActions, NumberOfTopics);
                    
                    var topAction = dataGen.GenerateRandomRankedActions(context).First();
                    var cost = dataGen.GenerateProbability() > 0.5 ? 0.0f : -1.0f;

                    trainingData.Add(new TrainingData() { Action = (int)topAction, Cost = cost, Probability = dataGen.GenerateProbability(), Context = context });
                }

                var trainStopwatch = new Stopwatch();

                var trainingParams = string.Format(TrainingParamsFormat, SavedModelFileVW);
                vw = new VowpalWabbit<UserContext<DocumentFeatures>, DocumentFeatures>(new VowpalWabbitSettings(trainingParams) { EnableExampleCaching = false });

                foreach (var td in trainingData)
                {
                    var context = td.Context as UserContext<DocumentFeatures>;
                    var label = new ContextualBanditLabel()
                    {
                        Action = (uint)td.Action,
                        Cost = td.Cost,
                        Probability = td.Probability
                    };

                    trainStopwatch.Start();
                    vw.Learn(context, context.ActionDependentFeatures, td.Action - 1, label);
                    trainStopwatch.Stop();
                }

                vw.Native.SaveModel();
                vw.Dispose();

                System.Console.WriteLine("Training data: {0} VW training time per sample: {1} ms", trainingData.Count, (double)trainStopwatch.ElapsedMilliseconds / (double)trainingData.Count);
            }
            catch (Exception ex)
            {
                System.Console.WriteLine(ex.Message);
                testPassed = false;
            }
            finally
            {
                if (vw != null)
                {
                    vw.Dispose();
                }
            }

            return testPassed;
        }
    }

    public class ObservationData
    {
        [JsonProperty(PropertyName = "i")]
        public string DocId { get; set; }

        [JsonProperty(PropertyName = "r")]
        public float Reward { get; set; }
    }

    public class DataGeneration
    {
        private const string TitleTemplate = "Your privacy is important to us. This privacy statement explains what personal data we collect from you and how we use it. It applies to Bing, Cortana, MSN, Office, OneDrive, Outlook.com, Skype, Windows, Xbox and other Microsoft services that display this statement. References to Microsoft services in this statement include Microsoft websites, apps, software and devices.";

        private Random rand;

        public DataGeneration()
        {
            this.rand = new Random();
        }

        public float GenerateProbability()
        {
            return (float)this.rand.Next(1, 100) / 100.0f;
        }

        public string GenerateTitle()
        {
            return TitleTemplate.Substring(this.rand.Next(0, TitleTemplate.Length / 2), this.rand.Next(TitleTemplate.Length / 2));
        }

        public LDAFeatureVector GenerateRandomLdaVector(int numTopics)
        {
            var vectors = new float[numTopics];
            for (var i = 0; i < numTopics; i++)
            {
                vectors[i] = Convert.ToSingle(this.rand.NextDouble());
            }

            return new LDAFeatureVector(vectors);
        }

        public ObservationData GenerateObservation(UserContext<DocumentFeatures> context)
        {
            var randAction = this.rand.Next(0, context.ActionDependentFeatures.Count);
            var clickedDoc = context.ActionDependentFeatures[randAction].Id;

            return new ObservationData()
            {
                DocId = clickedDoc,
                Reward = 1.0f
            };
        }

        public uint[] GenerateRandomRankedActions(UserContext<DocumentFeatures> context)
        {
            var numActions = context.ActionDependentFeatures.Count;
            var actions = new uint[numActions];

            for (uint i = 0; i < numActions; i++)
            {
                actions[i] = i + 1;
            }

            for (int i = 0; i < 5; i++)
            {
                var s1 = this.rand.Next(0, numActions);
                var s2 = this.rand.Next(0, numActions);

                var t = actions[s1];
                actions[s1] = actions[s2];
                actions[s2] = t;
            }

            return actions;
        }

        public UserContext<DocumentFeatures> GenerateRandomContext(int numActions, int numTopics, bool enableMissingUV = false)
        {
            var actionDF = new DocumentFeatures[numActions];
            var selected = this.rand.Next(0, numActions);

            var utcNow = DateTime.UtcNow;
            var minuteBucket = utcNow.Minute / 5;

            for (var i = 0; i < numActions; i++)
            {
                actionDF[i] = new DocumentFeatures();

                actionDF[i].Id = string.Format("AABB{0}{1}{2:D2}", utcNow.Hour, minuteBucket, i);
                actionDF[i].LDAVector = this.GenerateRandomLdaVector(numTopics);
                //actionDF[i].IsLocked = this.rand.NextDouble() > 0.5;
                actionDF[i].Title = this.GenerateTitle();
            }

            var context = new UserContext<DocumentFeatures>();

            context.User = new UserFeatures()
            {
                Age = Age.P,
                Gender = null,
                PassportAge = 20,
                Location = string.Empty
            };

            context.UserLDAVector = this.GenerateRandomLdaVector(numTopics);
            if (enableMissingUV && this.rand.Next(10) == 0)
            {
                context.UserLDAVector = null;
            }

            context.ActionDependentFeatures = actionDF;

            context.EventTime = DateTimeOffset.UtcNow;

            return context;
        }

        public string GetEventObjectString(string eventId, object eventData, IReferenceResolver resolver = null)
        {
            var jsonData = (resolver == null) ?
                JsonConvert.SerializeObject(eventData) :
                JsonConvert.SerializeObject(eventData, Formatting.None, new JsonSerializerSettings { ReferenceResolverProvider = () => resolver });

            StringBuilder json = new StringBuilder("{");

            json.AppendFormat("\"i\":\"{0}\",\"v\":{1}", eventId, jsonData);
            json.Append("}");

            return json.ToString();
        }

        public string BuildJoinMessage(IEnumerable<string> testobjectstrings)
        {
            StringBuilder jsonBuilder = new StringBuilder();

            jsonBuilder.Append("{\"i\":\"" + Guid.NewGuid().ToString() + "\",");

            jsonBuilder.Append("\"j\":[");
            jsonBuilder.Append(String.Join(",", testobjectstrings));
            jsonBuilder.Append("]}");

            return jsonBuilder.ToString();
        }
    }
}
