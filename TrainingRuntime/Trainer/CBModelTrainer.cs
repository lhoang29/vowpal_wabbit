namespace Microsoft.Content.Recommendations.TrainingRuntime.Trainer
{
    using System;
    using System.Collections.Generic;

    using Microsoft.Content.Recommendations.TrainingRuntime.Context;

    public class CBModelTrainer : Trainer<SimpleContext, SimpleActionDependentFeature>
    {
        private const string ModelParameters = "--cb {0} --cb_type dr";

        public CBModelTrainer(IDictionary<string, string> options = null)
            : base(options)
        {
            if (options != null && options.ContainsKey(TrainerOptions.Actions))
            {
                this.ActionsCount = Convert.ToInt32(TrainerOptions.Actions);
            }
        }

        public override string Id
        {
            get { return TrainerType.CBDR.ToString(); }
        }

        public int ActionsCount { get; private set; }

        public override void Initialize(string modelFile, bool loadInitialModel, string trackbackFile, bool enableTrackback)
        {
            var parameters = string.Format(ModelParameters, this.ActionsCount);

            base.Initialize(parameters, null, modelFile, loadInitialModel, trackbackFile, enableTrackback);
        }

        public override void Train(TrainingData trainingData)
        {
            if (trainingData == null)
            {
                return;
            }

            var input = string.Format("{0}:{1}:{2} | {3}", trainingData.Action, trainingData.Cost, trainingData.Probability, trainingData.Features ?? string.Empty);

            this.LearnExample(input, trainingData.Id);
        }
    }
}
