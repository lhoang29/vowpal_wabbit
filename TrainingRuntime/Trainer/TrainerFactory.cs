namespace Microsoft.Content.Recommendations.TrainingRuntime.Trainer
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    using Microsoft.Content.Recommendations.TrainingRuntime.Context;
    using VW;

    public static class TrainerFactory
    {
        public static ITrainer Create(TrainerType type, IDictionary<string, string> options)
        {
            ITrainer trainer = null;

            switch (type)
            {
                case TrainerType.CBDR:
                    trainer = new CBModelTrainer(options);
                    break;
            }

            return trainer;
        }

        public static ITrainer Create<TContext, TActionDependentFeature>(TrainerType type, IDictionary<string, string> options)
            where TContext : class, IActionDependentFeatureExample<TActionDependentFeature>
            where TActionDependentFeature : IActionDependentFeature
        {
            ITrainer trainer = null;

            switch (type)
            {
                case TrainerType.NOP:
                    trainer = new NopTrainer<TContext, TActionDependentFeature>(options);
                    break;

                case TrainerType.CBADF:
                    trainer = new CBMultiModelTrainer<TContext, TActionDependentFeature>(options);
                    break;
            }

            return trainer;
        }
    }
}
