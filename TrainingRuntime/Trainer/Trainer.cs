namespace Microsoft.Content.Recommendations.TrainingRuntime.Trainer
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    using Microsoft.Content.Recommendations.TrainingRuntime.Context;
    using VW;
    using VW.Serializer;
    using VW.Labels;

    public abstract class Trainer<TContext, TActionDependentFeature> : ITrainer
        where TContext : class, IActionDependentFeatureExample<TActionDependentFeature>
        where TActionDependentFeature : IActionDependentFeature
    {
        private const string FinalModelFileArgumentFormat = " -f {0}";

        private const string InitialModelFileArgumentFormat = " -i {0}";

        private const string InteractArgumentFormat = " --interact {0}";

        private bool isVWInitialized = false;

        private TrackbackManager trackbackManager;

        private VowpalWabbit<TContext, TActionDependentFeature> vw;

        private string modelFile;

        private string trackbackFile;

        private string customArguments = string.Empty;

        private string customResumeArguments = string.Empty;

        private bool trackbackEnabled = false;

        private bool generateModelId = true;

        private bool disposed = false;

        public Trainer(IDictionary<string, string> options = null)
        {
            if (options != null)
            {
                if (options.ContainsKey(TrainerOptions.Custom))
                {
                    this.customArguments = options[TrainerOptions.Custom];
                }

                if (options.ContainsKey(TrainerOptions.CustomResume))
                {
                    this.customResumeArguments = options[TrainerOptions.CustomResume];
                }

                if (options.ContainsKey(TrainerOptions.Interact))
                {
                    this.customArguments = string.Concat(this.customArguments, string.Format(InteractArgumentFormat, options[TrainerOptions.Interact]));
                }

                if (options.ContainsKey(TrainerOptions.GenerateModelId))
                {
                    this.generateModelId = bool.Parse(options[TrainerOptions.GenerateModelId]);
                }
            }
        }

        ~Trainer()
        {
            this.Dispose(false);
        }

        public abstract string Id
        {
            get;
        }

        public bool IsInitialized
        {
            get { return this.isVWInitialized; }
        }

        public bool IsInitializedWithSeed { get; private set; }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public abstract void Initialize(string modelFile, bool loadInitialModel, string trackbackFile = null, bool enableTrackback = false);

        public abstract void Train(TrainingData trainingData);

        public void Finish()
        {
            if (this.isVWInitialized)
            {
                this.vw.Dispose();
                this.isVWInitialized = false;
            }
        }

        public void SaveModel(string modelId = null)
        {
            if (!this.isVWInitialized)
            {
                return;
            }

            if (this.generateModelId)
            {
                this.vw.Native.ID = (modelId == null) ? Guid.NewGuid().ToString(): modelId;
            }

            this.vw.Native.SaveModel();

            if (this.trackbackEnabled && !string.IsNullOrEmpty(this.trackbackFile))
            {
                try
                {
                    using (var fs = File.Create(this.trackbackFile))
                    {
                        this.trackbackManager.Save(fs);
                    }

                    var fileInfo = new FileInfo(this.trackbackFile);
                    if (fileInfo.Length == 0)
                    {
                        File.Delete(this.trackbackFile);
                    }
                }
                catch (Exception ex)
                {
                    // non-critical
                    Trace.TraceError("Error saving trackback file: " + ex.Message);
                }

                this.trackbackManager.Reset();
            }
        }

        public void Reload()
        {
            if (!this.isVWInitialized)
            {
                return;
            }

            if (this.trackbackEnabled)
            {
                this.trackbackManager.Log(Constants.TrackbackReloadEventName);
            }

            this.vw.Native.Reload();
        }

        public void LogTrackback(string message)
        {
            if (this.isVWInitialized && this.trackbackEnabled)
            {
                this.trackbackManager.Log(message);
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposed)
            {
                return;
            }

            this.Finish();
      
            disposed = true;
        }

        protected void LearnExample(TContext context, int index, ILabel label, string exampleId = null)
        {
            if (!this.isVWInitialized)
            {
                return;
            }

            if (index >= context.ActionDependentFeatures.Count)
            {
                return;
            }

            if (this.trackbackEnabled)
            {
                this.trackbackManager.Log(exampleId);
            }

            this.vw.Learn(context, context.ActionDependentFeatures, index, label);
        }

        protected void LearnExample(string exampleLine, string exampleId = null)
        {
            if (!this.isVWInitialized)
            {
                return;
            }

            if (this.trackbackEnabled)
            {
                this.trackbackManager.Log(exampleId);
            }

            this.vw.Native.Learn(exampleLine);
        }

        protected void Initialize(string initString, string resumeInitString, string modelFile, bool loadInitialModel, string trackbackFile = null, bool enableTrackback = false)
        {
            if (this.isVWInitialized)
            {
                this.Finish();
            }

            this.trackbackManager = new TrackbackManager(Constants.MaxTrackbackMemUsage);
            this.trackbackEnabled = enableTrackback;
            this.trackbackFile = trackbackFile;
            if (File.Exists(this.trackbackFile))
            {
                File.Delete(this.trackbackFile);
            }

            this.modelFile = modelFile;
            this.IsInitializedWithSeed = false;

            if (!string.IsNullOrEmpty(modelFile))
            {
                if (!string.IsNullOrEmpty(this.customResumeArguments))
                {
                    resumeInitString = string.Concat(resumeInitString ?? string.Empty, " ", this.customResumeArguments);
                }

                if (loadInitialModel && File.Exists(modelFile))
                {
                    initString = string.Concat(resumeInitString ?? string.Empty, string.Format(InitialModelFileArgumentFormat, modelFile));
                    this.IsInitializedWithSeed = true;
                }
                else if (!string.IsNullOrEmpty(this.customArguments))
                {
                    initString = string.Concat(initString, " ", this.customArguments);
                }
                
                initString = string.Concat(initString, string.Format(FinalModelFileArgumentFormat, modelFile));
            }

            this.vw = new VowpalWabbit<TContext, TActionDependentFeature>(new VowpalWabbitSettings(initString) { EnableExampleCaching = false });
            if (this.vw != null)
            {
                Trace.TraceInformation(string.Format("Initialize VW with parameters: {0}", initString ?? string.Empty));

                if (this.trackbackEnabled)
                {
                    this.trackbackManager.Log(Constants.TrackbackInfoEventName + initString);
                }

                this.isVWInitialized = true;
            }
        }
    }
}
