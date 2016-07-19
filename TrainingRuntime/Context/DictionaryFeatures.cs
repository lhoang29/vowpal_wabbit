namespace Microsoft.Content.Recommendations.TrainingRuntime.Context
{
    using System;

    using Newtonsoft.Json;
    using VW.Labels;
    using VW.Serializer.Attributes;
    using System.Runtime.Serialization;
    using ProtoBuf;
    using System.Collections.Generic;


    /// <summary>
    /// Document features
    /// </summary>
    [Cacheable]
    [ProtoContract]
    public class DictionaryFeatures : IActionDependentFeature
    {
        // The full object is shared in between
        [Feature(Enumerize = true, FeatureGroup = 'p')]
        [JsonIgnore]
        public int EditorialPosition { get; set; }

        /// <summary>
        /// Id
        /// </summary>
        [Feature(FeatureGroup = 'i')]
        [JsonProperty(PropertyName = "i")]
        public string Id { get; set; }

        /// <summary>
        /// Document title
        /// </summary>
        [JsonProperty(PropertyName = "t")]
        [ProtoMember(1)]
        public string Title { get; set; }

        /// <summary>
        /// Document type
        /// </summary>
        [JsonProperty(PropertyName = "y")]
        [ProtoMember(2)]
        public DocumentType Type { get; set; }

        /// <summary>
        /// Document author
        /// </summary>
        [JsonProperty(PropertyName = "a")]
        [ProtoMember(3)]
        public string Author { get; set; }

        /// <summary>
        /// Document source identifier
        /// </summary>
        [JsonProperty(PropertyName = "s")]
        [ProtoMember(4)]
        public string Source { get; set; }

        /// <summary>
        /// Last modified date time
        /// </summary>
        [JsonProperty(PropertyName = "m")]
        [ProtoMember(5)]
        public DateTime LastModified { get; set; }

        /// <summary>
        /// Gets or sets the lock status of the document
        /// </summary>
        [JsonProperty(PropertyName = "k")]
        [ProtoMember(6)]
        public bool IsLocked { get; set; }

        /// <summary>
        /// Document LDA vector
        /// </summary>
        [Feature(FeatureGroup = 'd')] // , Dictify = true
        [JsonProperty(PropertyName = "v")]
        [ProtoMember(7)]
        public string LDAFeatureKey { get; set; }

        /// <summary>
        /// Compare whether this object represents the same document features as obj
        /// </summary>
        /// <param name="obj">the object to compare</param>
        /// <returns>true if the same</returns>
        public override bool Equals(object obj)
        {
            var other = obj as DocumentFeatures;
            if (other == null)
            {
                return false;
            }

            return this.Id.Equals(other.Id) && this.LastModified.Equals(other.LastModified);
        }

        /// <summary>
        /// Returns the hash code for the object.
        /// </summary>
        /// <returns>hash</returns>
        public override int GetHashCode()
        {
            return this.Id.GetHashCode() ^ this.LastModified.GetHashCode();
        }

        public static DictionaryFeatures CreateFromDocumentFeatures(
            DocumentFeatures doc,
            Dictionary<string, int> ldaToIndex)
        {
            return new DictionaryFeatures
            { 
                EditorialPosition = doc.EditorialPosition,
                Id = doc.Id,
                Title = doc.Title,
                Type = doc.Type,
                Author = doc.Author,
                Source = doc.Source,
                LastModified = doc.LastModified,
                IsLocked = doc.IsLocked,
                LDAFeatureKey = ldaToIndex[doc.GetLDAString()].ToString()
            };
        }
    }
}
