namespace Microsoft.Content.Recommendations.TrainingRuntime.RewardFunction
{
    public interface IRewardFunction
    {
        string Id { get; }

        double GetCost(object context, int[] actions, double probability, int observedAction, double reward);

        double GetNoObservationCost(object context, int[] actions, double probability);
    }
}
