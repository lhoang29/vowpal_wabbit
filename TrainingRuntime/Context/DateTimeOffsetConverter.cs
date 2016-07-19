namespace Microsoft.Content.Recommendations.TrainingRuntime.Context
{
    using System;

    using Newtonsoft.Json;


    public sealed class DateTimeOffsetConverter : JsonConverter
    {
        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof(Nullable<DateTimeOffset>);
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            if (reader.Value == null || reader.ValueType != typeof(string))
            {
                return null;
            }

            DateTimeOffset dto;
            if (DateTimeOffset.TryParse((string)reader.Value, out dto))
            {
                return dto;
            }

            return null;
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            throw new NotImplementedException();
        }
    }
}
