namespace Microsoft.Content.Recommendations.TrainingRuntime.Trainer
{
    using System;


    public interface ITrainer : IDisposable
    {
        string Id { get; }

        bool IsInitialized { get; }

        bool IsInitializedWithSeed { get; }

        void Initialize(string modelFile, bool loadInitialModel, string trackbackFile = null, bool enableTrackback = false);

        void Train(TrainingData trainingData);

        void Finish();

        void SaveModel(string modelId = null);

        void Reload();

        void LogTrackback(string message);
    }
}
