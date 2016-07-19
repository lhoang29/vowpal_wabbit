namespace Microsoft.Content.Recommendations.TrainingRuntime.Context
{
    using System;
    using System.Collections.Generic;

    using Newtonsoft.Json;
    using VW;
    using VW.Serializer.Attributes;
    using ProtoBuf;


    [ProtoContract]
    public class UserContext<TActionDependentFeature>: IActionDependentFeatureExample<TActionDependentFeature>
        where TActionDependentFeature: IActionDependentFeature
    {
        //[Feature]
        //public int Dummy { get; set; }

        /// <summary>
        /// Any user feature other than LDA user topics
        /// </summary>
        [Feature(Namespace = "otheruser", FeatureGroup = 'o')]
        [JsonProperty(PropertyName = "uf")]
        [ProtoMember(1)]
        public UserFeatures User { get; set; }

        /// <summary>
        /// LDA user topics, need a seperate namespace
        /// </summary>
        [Feature(Namespace = "userlda", FeatureGroup = 'u')]
        [JsonProperty(PropertyName = "ul")]
        [ProtoMember(2, AsReference = true)]
        public LDAFeatureVector UserLDAVector { get; set; }

        /// <summary>
        /// List of documents in this interaction
        /// </summary>
        [JsonProperty(PropertyName = "adf", ItemIsReference = true)]
        [ProtoMember(3)]
        public IReadOnlyList<TActionDependentFeature> ActionDependentFeatures { get; set; }
        // public List<DocumentFeatures> ActionDependentFeatures { get; set; }

        /// <summary>
        /// Gets or sets the model id
        /// </summary>
        [JsonProperty(PropertyName = "mi")]
        [ProtoMember(4)]
        public string ModelId { get; set; }

        /// <summary>
        /// Time of this interaction event
        /// </summary>
        [JsonProperty(PropertyName = "et")]
        [ProtoMember(5)]
        public DateTimeOffset? EventTime { get; set; }

        /// <summary>
        /// Hour component for this event from 1 to 24 or 0 if we don't have time information
        /// </summary>
        [JsonIgnore]
        public int TimeOfDay
        {
            get
            {
                if (this.EventTime == null || !this.EventTime.HasValue)
                {
                    return 0;
                }

                return 1 + this.EventTime.Value.Hour;
            }
        }

        /// <summary>
        /// Day of week for this event from 1 (Sun) to 7 (Sat) or 0 if we don't have time information
        /// </summary>
        [JsonIgnore]
        public int DayOfWeek
        {
            get
            {
                if (this.EventTime == null || !this.EventTime.HasValue)
                {
                    return 0;
                }

                return 1 + (int)this.EventTime.Value.DayOfWeek;
            }
        }
    }
}
