namespace Microsoft.Content.Recommendations.TrainingRuntime.Trainer
{
    /// <summary>
    /// Training data
    /// </summary>
    public class TrainingData
    {
        /// <summary>
        /// Training data identifier
        /// </summary>
        public string Id { get; set; }

        /// <summary>
        /// Top action chosen
        /// </summary>
        public int Action { get; set; }

        /// <summary>
        /// Cost of the action
        /// </summary>
        public float Cost { get; set; }

        /// <summary>
        /// Probability of selecting the action
        /// </summary>
        public float Probability { get; set; }

        /// <summary>
        /// Features to feed into VW
        /// </summary>
        public string Features { get; set; }

        /// <summary>
        /// Annotated context object to feed into VW
        /// </summary>
        public object Context { get; set; }
    }
}
