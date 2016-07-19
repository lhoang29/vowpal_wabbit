namespace Microsoft.Content.Recommendations.TrainingRuntime.Reader
{
    using System;

    using Microsoft.Content.Recommendations.TrainingRuntime.Context;
    using Microsoft.Content.Recommendations.TrainingRuntime.RewardFunction;
    using Microsoft.WindowsAzure.Storage;


    public class DecisionServiceUserContextDataReader: DecisionServiceReader<UserContext<DocumentFeatures>, DocumentFeatures>
    {
        public DecisionServiceUserContextDataReader(string appId, CloudStorageAccount storageAccount, int refCapacity, IRewardFunction rewardFunction, DateTime? startTimeInclusive = null, DateTime? endTimeExclusive = null, int maxTDQueueCount = 0)
            : base(appId, storageAccount, refCapacity, rewardFunction, startTimeInclusive, endTimeExclusive, maxTDQueueCount)
        {
        }
    }
}
