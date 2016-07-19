namespace Microsoft.Content.Recommendations.TrainingRuntime.RewardFunction
{
    public class ClickRewardFunction: IRewardFunction
    {
        public string Id
        {
            get
            {
                return "ClickReward";
            }
        }

        public double GetCost(object context, int[] actions, double probability, int observedAction, double reward)
        {
            if (actions[0] == observedAction)
            {
                return -1.0;
            }

            return 0.0;
        }

        public double GetNoObservationCost(object context, int[] actions, double probability)
        {
            return 0.0;
        }
    }
}
