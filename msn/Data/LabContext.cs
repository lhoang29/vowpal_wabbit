using Microsoft.Hadoop.Avro;
using Newtonsoft.Json;
using ProtoBuf;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace msn
{
    [ProtoContract]
    [DataContract]
    public class Label
    {
        [ProtoMember(1)]
        [JsonProperty(PropertyName = "a")]
        [DataMember]
        public int Action { get; set; }

        [ProtoMember(2)]
        [JsonProperty(PropertyName = "p")]
        [DataMember]
        public float Probability { get; set; }

        [ProtoMember(3)]
        [JsonProperty(PropertyName = "c")]
        [DataMember]
        public float Cost { get; set; }
    }

    [ProtoContract]
    [DataContract]
    public class LabContext
    {
        [ProtoMember(1)]
        [JsonProperty(PropertyName = "e")]
        [DataMember]
        public Label Label { get; set; }

        [ProtoMember(2)]
        [JsonProperty(PropertyName = "g", NullValueHandling = NullValueHandling.Ignore)]
        [DataMember(IsRequired = false)]
        [NullableSchema]
        //public int? AgeGroup { get; set; }
        public Age? AgeGroup { get; set; }

        [ProtoMember(3)]
        [JsonProperty(PropertyName = "a", NullValueHandling = NullValueHandling.Ignore)]
        [NullableSchema]
        [DataMember(IsRequired = false)]
        public int? Age { get; set; }

        [ProtoMember(4)]
        [JsonProperty(PropertyName = "s", NullValueHandling = NullValueHandling.Ignore)]
        [NullableSchema]
        [DataMember(IsRequired = false)]
        public Gender? Gender { get; set; }
        //public int? Gender { get; set; }

        [ProtoMember(5)]
        [JsonProperty(PropertyName = "l", NullValueHandling = NullValueHandling.Ignore)]
        [NullableSchema]
        [DataMember(IsRequired = false)]
        public string Location { get; set; }

        [ProtoMember(6)]
        [JsonProperty(PropertyName = "v", NullValueHandling = NullValueHandling.Ignore)]
        [DataMember]
        public long PageViews { get; set; }

        [ProtoMember(7)]
        [JsonProperty(PropertyName = "u")]
        //[DataMember]
        [IgnoreDataMember]
        public float[] UserLDA { get; set; }

        [ProtoMember(8)]
        [JsonProperty(PropertyName = "d")]
        [DataMember]
        public Document[] Documents { get; set; }
    }

    [ProtoContract]
    [DataContract]
    public class Document
    {
        [ProtoMember(1)]
        [JsonProperty(PropertyName = "i", NullValueHandling = NullValueHandling.Ignore)]
        [DataMember]
        public string Id { get; set; }

        [ProtoMember(2)]
        [JsonProperty(PropertyName = "s", NullValueHandling = NullValueHandling.Ignore)]
        [DataMember(IsRequired = false)]
        [NullableSchema]
        public string Source { get; set; }

        [IgnoreDataMember]
        [ProtoMember(3, AsReference = true)]
        [JsonProperty(PropertyName = "d", IsReference = true)]
        public float[] DocumentLDA { get; set; }

        [IgnoreDataMember]
        //[DataMember]
        public Guid DocumentLDAId { get; set; }
    }

    [ProtoContract]
    [DataContract]
    public enum Age
    {
        /// <summary>
        /// 0-17
        /// </summary>
        A,

        /// <summary>
        /// 18-24
        /// </summary>
        B,

        /// <summary>
        /// 25-34
        /// </summary>
        C,

        /// <summary>
        /// 35-49
        /// </summary>
        D,

        /// <summary>
        /// 50+
        /// </summary>
        E
    }

    [ProtoContract]
    [DataContract]
    public enum Gender
    {
        Female,

        Male
    }
}
