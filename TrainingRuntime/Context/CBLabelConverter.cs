namespace Microsoft.Content.Recommendations.TrainingRuntime.Context
{
    using System;

    using Newtonsoft.Json;
    using VW.Labels;


    public sealed class CBLabelConverter : JsonConverter
    {
        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof(ILabel);
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            return serializer.Deserialize<ContextualBanditLabel>(reader);
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            serializer.Serialize(writer, value);
        }
    }
}
