namespace Microsoft.Content.Recommendations.TrainingRuntime.Reader
{
    using Microsoft.Content.Recommendations.TrainingRuntime.RewardFunction;
    using Microsoft.Content.Recommendations.TrainingRuntime.Trainer;

    public interface IDataReader
    {
        int CachedReferencesCount { get; }

        void Reset();

        void ResumeRead();

        bool MoveNext();

        void MoveToEnd();

        TrainingData GetCurrentTrainingData();
    }
}
