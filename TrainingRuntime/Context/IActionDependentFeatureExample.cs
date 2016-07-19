namespace Microsoft.Content.Recommendations.TrainingRuntime.Context
{
    using System.Collections.Generic;

    public interface IActionDependentFeatureExample<T>
        where T : IActionDependentFeature
    {
        IReadOnlyList<T> ActionDependentFeatures { get; set; }
    }
}
