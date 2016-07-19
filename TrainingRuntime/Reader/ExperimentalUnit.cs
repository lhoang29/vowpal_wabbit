namespace Microsoft.Content.Recommendations.TrainingRuntime.Reader
{
    using System;
    using System.Collections.Generic;

    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;


    /// <summary>
    /// A completed experimental unit containing multiple fragments (interactions/observations).
    /// </summary>
    [JsonConverter(typeof(ExperimentalUnitConverter))]
    public class ExperimentalUnit: IExperimentalUnit
    {
        /// <summary>
        /// Gets or sets the experimental unit ID.
        /// </summary>
        public string Id { get; set; }

        /// <summary>
        /// Gets or sets the interactions
        /// </summary>
        public IList<Interaction<JToken>> Interactions { get; set; }

        /// <summary>
        /// Gets or sets the observations
        /// </summary>
        public IList<JToken> Observations { get; set; }

        public void AddSingleActionInteraction(int action, double probability, object context)
        {
            this.Interactions = this.Interactions ?? new List<Interaction<JToken>>();

            var interaction = new Interaction<JToken>()
            {
                Action = action,
                Probability = probability,
                Context = (JToken)context
            };

            this.Interactions.Add(interaction);
        }

        public void AddMultiActionInteraction(int[] actions, double probability, object context)
        {
            this.Interactions = this.Interactions ?? new List<Interaction<JToken>>();

            var interaction = new Interaction<JToken>()
            {
                Actions = actions,
                Probability = probability,
                Context = (JToken)context
            };

            this.Interactions.Add(interaction);
        }

        public void AddObservation(object observation)
        {
            this.Observations = this.Observations ?? new List<JToken>();
            this.Observations.Add((JToken)observation);
        }
    }
}
