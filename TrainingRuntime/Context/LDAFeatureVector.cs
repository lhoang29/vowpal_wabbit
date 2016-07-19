namespace Microsoft.Content.Recommendations.TrainingRuntime.Context
{
    using Microsoft.Content.Recommendations.LinearAlgebra;
    using Newtonsoft.Json;
    using ProtoBuf;
    using VW.Serializer.Attributes;


    [ProtoContract]
    public class LDAFeatureVector
    {
        private DenseVector denseVector;

        public const bool UseAnchor = true;

        public LDAFeatureVector()
        {
        }

        public LDAFeatureVector(int length, float initValue = 0)
        {
            if (length > 0)
            {
                var v = new float[length];
                if (initValue != 0)
                {
                    for (var i = 0; i < v.Length; i++)
                    {
                        v[i] = initValue;
                    }
                }

                this.Vectors = v;
            }
        }

        public LDAFeatureVector(float[] vector)
        {
            this.Vectors = vector;
        }

        /// <summary>
        /// Base64 representation
        /// </summary>
        [JsonProperty(PropertyName = "e")]
        [ProtoMember(1)]
        public string Encoded { get; set; }

        /// <summary>
        /// Array of floats as features to VW
        /// </summary>
        [Feature(AddAnchor = UseAnchor)]
        [JsonIgnore]
        public float[] Vectors
        {
            get
            {
                if (string.IsNullOrEmpty(this.Encoded))
                {
                    return null;
                }

                if (this.denseVector == null)
                {
                    var tempVector = new DenseVector();
                    tempVector.Deserialize(this.Encoded);
                    this.denseVector = tempVector;
                }

                return this.denseVector.ToArray();
            }

            private set
            {
                if (value != null)
                {
                    this.denseVector = new DenseVector(value);
                    this.Encoded = this.denseVector.Serialize(VectorType.DenseVector, SerializationEncoding.Compressed);
                }
                else
                {
                    this.denseVector = null;
                }
            }
        }
    }
}
