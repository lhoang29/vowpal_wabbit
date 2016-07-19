namespace Microsoft.Content.Recommendations.TrainingRuntime.Reader
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Reflection;
    using System.Text;
    using System.Threading.Tasks;

    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    /// <summary>
    /// Deserialization helper to map to Interaction/Observation based on "t" property.
    /// </summary>
    internal sealed class ExperimentalUnitConverter : JsonConverter
    {
        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof(ExperimentalUnit);
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            var jObject = JObject.Load(reader);

            var experimentalUnit = Activator.CreateInstance(objectType) as IExperimentalUnit;

            var unitId = (string)jObject["i"];
            var fragments = jObject["f"] as JArray;

            if (string.IsNullOrEmpty(unitId) || fragments == null)
            {
                // not a valid exp unit json object, return an empty exp unit
                return experimentalUnit;
            }

            experimentalUnit.Id = unitId;

            foreach (var fragment in fragments)
            {
                var type = fragment.Value<int>("t");

                switch (type)
                {
                    case 0:
                    {
                        var action = fragment.Value<int>("a");
                        var probability = fragment.Value<double>("p");
                        var context = fragment["c"];

                        experimentalUnit.AddSingleActionInteraction(action, probability, context);
                        break;
                    }

                    case 1:
                    {
                        experimentalUnit.AddObservation(fragment["v"]);
                        break;
                    }

                    case 2:
                    {
                        var actions = fragment["a"].ToObject<int[]>();
                        var probability = fragment.Value<double>("p");
                        var context = fragment["c"];

                        experimentalUnit.AddMultiActionInteraction(actions, probability, context);
                        break;
                    }

                    default:
                        // Skip for now
                        Trace.TraceWarning("Skipping fragment with type: " + type);
                        break;
                }
            }

            return experimentalUnit;
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            ExperimentalUnit unit = value as ExperimentalUnit;
            IEnumerable<ExperimentalUnit> units = null;

            if (unit == null)
            {
                units = value as IEnumerable<ExperimentalUnit>;
            }

            if (unit == null && units == null)
            {
                return;
            }

            if (unit != null)
            {
                this.WriteUnit(unit, writer, serializer);
            }
            else
            {
                writer.WriteStartArray();

                foreach (var u in units)
                {
                    this.WriteUnit(u, writer, serializer);
                }

                writer.WriteEndArray();
            }
        }

        private void WriteUnit(ExperimentalUnit unit, JsonWriter writer, JsonSerializer serializer)
        {
            writer.WriteStartObject();

            // exp id
            writer.WritePropertyName("i");
            writer.WriteValue(unit.Id);

            // exp fragments
            writer.WritePropertyName("f");
            writer.WriteStartArray();
            writer.WriteEndArray();

            if (unit.Interactions != null)
            {
                foreach (var interaction in unit.Interactions)
                {
                    writer.WritePropertyName("t");
                    writer.WriteValue(interaction.IsMultiAction? 2 : 0);

                    writer.WritePropertyName("a");
                    if (interaction.IsMultiAction)
                    {
                        serializer.Serialize(writer, interaction.Actions);
                    }
                    else
                    {
                        writer.WriteValue(interaction.Action);
                    }

                    writer.WritePropertyName("p");
                    writer.WriteValue(interaction.Probability);

                    writer.WritePropertyName("c");
                    writer.WriteValue(interaction.Context);
                }
            }

            if (unit.Observations != null)
            {
                foreach (var observation in unit.Observations)
                {
                    writer.WritePropertyName("t");
                    writer.WriteValue(1);

                    writer.WritePropertyName("v");
                    writer.WriteValue(observation);
                }
            }

            writer.WriteEndObject();
        }
    }
}
