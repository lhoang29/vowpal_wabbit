namespace Microsoft.Content.Recommendations.TrainingRuntime.Reader
{
    using System.Linq;

    /// <summary>
    /// Base class for interaction events.
    /// </summary>
    public class Interaction<TContext>
    {
        /// <summary>
        /// The chosen action.
        /// </summary>
        public int Action { get; set; }

        /// <summary>
        /// The list of chosen actions.
        /// </summary>
        public int[] Actions { get; set; }

        /// <summary>
        /// Probability of chosen action.
        /// </summary>
        public double Probability { get; set; }

        /// <summary>
        /// The context of the interaction.
        /// </summary>
        public TContext Context { get; set; }

        public bool IsMultiAction
        {
            get
            {
                return Actions != null;
            }
        }

        /// <summary>
        /// Returns whether the specified object contains the same data.
        /// </summary>
        /// <param name="obj">The other object.</param>
        /// <returns>bool</returns>
        public override bool Equals(object obj)
        {
            var other = obj as Interaction<TContext>;
            if (other == null)
            {
                return false;
            }

            return (this.IsMultiAction ? this.Actions.SequenceEqual(other.Actions) : (this.Action == other.Action)) &&
                   this.Probability == other.Probability &&
                   this.Context.Equals(other.Context);
        }

        /// <summary>
        /// Returns the hash code for the object.
        /// </summary>
        /// <returns>int.</returns>
        public override int GetHashCode()
        {
            return (this.IsMultiAction ? this.Actions.GetHashCode() : this.Action.GetHashCode()) ^
                   this.Probability.GetHashCode() ^
                   this.Context.GetHashCode();
        }
    }
}
