namespace Microsoft.Content.Recommendations.TrainingRuntime.Context
{
    using System;
    using System.Collections.Generic;

    using Newtonsoft.Json;


    public class SimpleContext : IActionDependentFeatureExample<SimpleActionDependentFeature>
    {
        [JsonProperty(PropertyName = "adf")]
        public IReadOnlyList<SimpleActionDependentFeature> ActionDependentFeatures { get; set; }
    }

    public class SimpleActionDependentFeature : IActionDependentFeature
    {
        public string Id { get; set; }
    }
}
