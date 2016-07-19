namespace Microsoft.Content.Recommendations.TrainingRuntime.Reader
{
    interface IExperimentalUnit
    {
        string Id { get; set; }

        void AddSingleActionInteraction(int action, double probability, object context);

        void AddMultiActionInteraction(int[] actions, double probability, object context);

        void AddObservation(object observation);
    }
}
