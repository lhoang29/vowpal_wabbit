namespace Microsoft.Content.Recommendations.TrainingRuntime.Context
{
    using System;
    using System.Collections.Generic;

    using Newtonsoft.Json;
    using VW.Serializer;
    using VW.Serializer.Attributes;
    using ProtoBuf;


    public enum Gender
    {
        /// <summary>
        /// female
        /// </summary>
        Female,

        /// <summary>
        /// male
        /// </summary>
        Male
    }

    /// <summary>
    /// Enum for Age
    /// </summary>
    public enum Age
    {
        /// <summary>
        /// 0-17
        /// </summary>
        O,

        /// <summary>
        /// 18-24
        /// </summary>
        P,

        /// <summary>
        /// 25-34
        /// </summary>
        Q,

        /// <summary>
        /// 35-49
        /// </summary>
        R,

        /// <summary>
        /// 50+
        /// </summary>
        T
    }

    [ProtoContract]
    public class UserFeatures
    {
        /// <summary>
        /// Age Group, the value could be null
        /// </summary>
        [Feature]
        [JsonProperty(PropertyName = "a")]
        [ProtoMember(1)]
        public Age? Age { get; set; }

        /// <summary>
        /// Exact Age, the value could be null
        /// </summary>
        [Feature(Enumerize = true)]
        [JsonProperty(PropertyName = "p")]
        [ProtoMember(2)]
        public int? PassportAge { get; set; }

        /// <summary>
        /// Gender, the value could be null
        /// </summary>
        [Feature]
        [JsonProperty(PropertyName = "g")]
        [ProtoMember(3)]
        public Gender? Gender { get; set; }

        /// <summary>
        /// Location/State Name, the value could be null or empty
        /// </summary>
        [Feature(StringProcessing = StringProcessing.Escape)]
        [JsonProperty(PropertyName = "l")]
        [ProtoMember(4)]
        public string Location { get; set; }

        /// <summary>
        /// Total page views of this user on prime
        /// </summary>
        [Feature]
        [JsonProperty(PropertyName = "v")]
        [ProtoMember(5)]
        public long PrimePageViews { get; set; }

        /// <summary>
        /// Key-value pairs from Concept Hierarchical Engine
        /// key : category id
        /// value: confidence value between 0 and 1
        /// </summary>
        [JsonProperty(PropertyName = "c")]
        [ProtoMember(6)]
        public Dictionary<int, float> CHEData { get; set; }
    }
}
