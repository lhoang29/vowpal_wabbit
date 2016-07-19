namespace Microsoft.Content.Recommendations.TrainingRuntime.Trainer
{
    using System;
    using System.Collections.Generic;

    using Microsoft.Content.Recommendations.TrainingRuntime.Context;
    using VW;


    public class NopTrainer<TContext, TActionDependentFeature> : Trainer<TContext, TActionDependentFeature>
        where TContext : class, IActionDependentFeatureExample<TActionDependentFeature>
        where TActionDependentFeature : IActionDependentFeature
    {
        private const string ModelParameters = "--cb_adf --rank_all";

        public NopTrainer(IDictionary<string, string> options = null)
            : base(options)
        {
        }

        public override string Id
        {
            get { return TrainerType.NOP.ToString(); }
        }

        public override void Initialize(string modelFile, bool loadInitialModel, string trackbackFile, bool enableTrackback)
        {
            base.Initialize(ModelParameters, null, modelFile, loadInitialModel, trackbackFile, enableTrackback);
        }

        public override void Train(TrainingData trainingData)
        {
        }
    }
}
