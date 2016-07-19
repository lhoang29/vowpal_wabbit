namespace Microsoft.Content.Recommendations.TrainingRuntime.Context
{
    using System;

    using Newtonsoft.Json;
    using VW.Serializer.Attributes;
    using VW.Labels;

    public abstract class ActionDependentFeatureBase
    {
        /// <summary>
        /// Id
        /// </summary>
        [Feature]
        [JsonProperty(PropertyName = "i")]
        public string Id { get; set; }

        /// <summary>
        /// Label for training
        /// </summary>
        [JsonProperty(PropertyName = "b")]
        [JsonConverter(typeof(CBLabelConverter))]
        public ILabel Label { get; set; }
    }
}
