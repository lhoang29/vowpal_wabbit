namespace Microsoft.Content.Recommendations.TrainingRuntime.Trainer
{
    using System;
    using System.Collections.Generic;

    using Microsoft.Content.Recommendations.TrainingRuntime.Context;
    using VW;
    using VW.Labels;

    public class CBMultiModelTrainer<TContext, TActionDependentFeature> : Trainer<TContext, TActionDependentFeature>
        where TContext : class, IActionDependentFeatureExample<TActionDependentFeature>
        where TActionDependentFeature : IActionDependentFeature
    {
        private const string ModelParameters = "--cb_adf --rank_all";

        private string id;

        public CBMultiModelTrainer(IDictionary<string, string> options = null)
            : base(options)
        {
            if (options != null && options.Count > 0)
            {
                this.id = string.Format("{0}-{1}", TrainerType.CBADF.ToString(), string.Join("+", options.Keys));
            }
            else
            {
                this.id = TrainerType.CBADF.ToString();
            }
        }

        public override string Id
        {
            get { return this.id; }
        }

        public override void Initialize(string modelFile, bool loadInitialModel, string trackbackFile, bool enableTrackback)
        {
            base.Initialize(ModelParameters, null, modelFile, loadInitialModel, trackbackFile, enableTrackback);
        }

        public override void Train(TrainingData trainingData)
        {
            if (trainingData == null || trainingData.Context == null)
            {
                return;
            }

            var tc = trainingData.Context as TContext;
            if (tc == null)
            {
                return;
            }

            // add label
            var topActionIdx = trainingData.Action - 1;
            var label = new ContextualBanditLabel()
            {
                Action = (uint)trainingData.Action,
                Cost = trainingData.Cost,
                Probability = trainingData.Probability
            };

            this.LearnExample(tc, topActionIdx, label, trainingData.Id);
        }
    }
}
