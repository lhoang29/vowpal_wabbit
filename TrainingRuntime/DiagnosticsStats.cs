namespace Microsoft.Content.Recommendations.TrainingRuntime
{
    using System;
    using System.Diagnostics;

    public class DiagnosticsStats
    {
        private static DiagnosticsStats Instance = null;

        private DiagnosticsStats()
        {
            if (PerformanceCounterCategory.Exists(Constants.PerfCounterCategory))
            {
                PerformanceCounterCategory.Delete(Constants.PerfCounterCategory);
            }

            if (!PerformanceCounterCategory.Exists(Constants.PerfCounterCategory))
            {
                CounterCreationDataCollection counterCollection = new CounterCreationDataCollection();

                counterCollection.Add(new CounterCreationData() { CounterName = "TotalTrainingDataCount", CounterHelp = "", CounterType = PerformanceCounterType.NumberOfItems64 });

                counterCollection.Add(new CounterCreationData() { CounterName = "UnresolvedRefCount", CounterHelp = "", CounterType = PerformanceCounterType.NumberOfItems64 });

                counterCollection.Add(new CounterCreationData() { CounterName = "InvalidTrainingDataCount", CounterHelp = "", CounterType = PerformanceCounterType.NumberOfItems64 });

                counterCollection.Add(new CounterCreationData() { CounterName = "TrainingExceptionCount", CounterHelp = "", CounterType = PerformanceCounterType.NumberOfItems32 });

                counterCollection.Add(new CounterCreationData() { CounterName = "ReaderExceptionCount", CounterHelp = "", CounterType = PerformanceCounterType.NumberOfItems32 });

                counterCollection.Add(new CounterCreationData() { CounterName = "ModelUploadFailedCount", CounterHelp = "", CounterType = PerformanceCounterType.NumberOfItems32 });

                counterCollection.Add(new CounterCreationData() { CounterName = "ExpUnitJsonErrorCount", CounterHelp = "", CounterType = PerformanceCounterType.NumberOfItems64 });

                counterCollection.Add(new CounterCreationData() { CounterName = "ReaderTimeoutCount", CounterHelp = "", CounterType = PerformanceCounterType.NumberOfItems64 });

                counterCollection.Add(new CounterCreationData() { CounterName = "CompleteUnitCount", CounterHelp = "", CounterType = PerformanceCounterType.NumberOfItems64 });

                counterCollection.Add(new CounterCreationData() { CounterName = "PositiveTrainingDataCount", CounterHelp = "", CounterType = PerformanceCounterType.NumberOfItems64 });

                counterCollection.Add(new CounterCreationData() { CounterName = "TrainingDataQueueCount", CounterHelp = "", CounterType = PerformanceCounterType.NumberOfItems64 });

                counterCollection.Add(new CounterCreationData() { CounterName = "DequeueTimeoutCount", CounterHelp = "", CounterType = PerformanceCounterType.NumberOfItems64 });

                counterCollection.Add(new CounterCreationData() { CounterName = "TrainingDataRate", CounterHelp = "", CounterType = PerformanceCounterType.RateOfCountsPerSecond64 });

                counterCollection.Add(new CounterCreationData() { CounterName = "AvgReaderEnumerateTime", CounterHelp = "", CounterType = PerformanceCounterType.AverageTimer32 });

                counterCollection.Add(new CounterCreationData() { CounterName = "AvgReaderEnumerateTimeBase", CounterHelp = "", CounterType = PerformanceCounterType.AverageBase });

                counterCollection.Add(new CounterCreationData() { CounterName = "AvgModelUploadTime", CounterHelp = "", CounterType = PerformanceCounterType.AverageTimer32 });

                counterCollection.Add(new CounterCreationData() { CounterName = "AvgModelUploadTimeBase", CounterHelp = "", CounterType = PerformanceCounterType.AverageBase });

                counterCollection.Add(new CounterCreationData() { CounterName = "AvgVWTrainingTime", CounterHelp = "", CounterType = PerformanceCounterType.AverageTimer32 });

                counterCollection.Add(new CounterCreationData() { CounterName = "AvgVWTrainingTimeBase", CounterHelp = "", CounterType = PerformanceCounterType.AverageBase });

                PerformanceCounterCategory.Create(Constants.PerfCounterCategory, "Training Service Perf Counters", PerformanceCounterCategoryType.SingleInstance, counterCollection);
            }

            this.TotalTrainingDataCounter = new PerformanceCounter(Constants.PerfCounterCategory, "TotalTrainingDataCount", false);
            this.TrainingExceptionCounter = new PerformanceCounter(Constants.PerfCounterCategory, "TrainingExceptionCount", false);
            this.ReaderExceptionCounter = new PerformanceCounter(Constants.PerfCounterCategory, "ReaderExceptionCount", false);
            this.UnresolvedRefCounter = new PerformanceCounter(Constants.PerfCounterCategory, "UnresolvedRefCount", false);
            this.InvalidTrainingDataCounter = new PerformanceCounter(Constants.PerfCounterCategory, "InvalidTrainingDataCount", false);
            this.ExpUnitJsonErrorCounter = new PerformanceCounter(Constants.PerfCounterCategory, "ExpUnitJsonErrorCount", false);
            this.ReaderTimeoutCounter = new PerformanceCounter(Constants.PerfCounterCategory, "ReaderTimeoutCount", false);
            this.CompleteUnitCounter = new PerformanceCounter(Constants.PerfCounterCategory, "CompleteUnitCount", false);
            this.PositiveTrainingDataCounter = new PerformanceCounter(Constants.PerfCounterCategory, "PositiveTrainingDataCount", false);
            this.TrainingDataQueueCounter = new PerformanceCounter(Constants.PerfCounterCategory, "TrainingDataQueueCount", false);
            this.DequeueTimeoutCounter = new PerformanceCounter(Constants.PerfCounterCategory, "DequeueTimeoutCount", false);

            this.TrainingDataRateCounter = new PerformanceCounter(Constants.PerfCounterCategory, "TrainingDataRate", false);
            this.AvgVWTrainingTimeCounter = new PerformanceCounter(Constants.PerfCounterCategory, "AvgVWTrainingTime", false);
            this.AvgVWTrainingTimeBaseCounter = new PerformanceCounter(Constants.PerfCounterCategory, "AvgVWTrainingTimeBase", false);

            this.AvgReaderEnumerateTimeCounter = new PerformanceCounter(Constants.PerfCounterCategory, "AvgReaderEnumerateTime", false);
            this.AvgReaderEnumerateTimeBaseCounter = new PerformanceCounter(Constants.PerfCounterCategory, "AvgReaderEnumerateTimeBase", false);

            this.ModelUploadFailedCounter = new PerformanceCounter(Constants.PerfCounterCategory, "ModelUploadFailedCount", false);
            this.AvgModelUploadTimeCounter = new PerformanceCounter(Constants.PerfCounterCategory, "AvgModelUploadTime", false);
            this.AvgModelUploadTimeBaseCounter = new PerformanceCounter(Constants.PerfCounterCategory, "AvgModelUploadTimeBase", false);
        }

        static DiagnosticsStats()
        {
            // Thread-safe
            Instance = new DiagnosticsStats();
        }

        public static DiagnosticsStats GetInstance()
        {
            //if (Instance == null)
            //{
            //    Instance = new DiagnosticsStats();
            //}

            return Instance;
        }

        public PerformanceCounter TotalTrainingDataCounter { get; set; }

        public PerformanceCounter TrainingExceptionCounter { get; set; }

        public PerformanceCounter ReaderExceptionCounter { get; set; }

        public PerformanceCounter UnresolvedRefCounter { get; set; }

        public PerformanceCounter InvalidTrainingDataCounter { get; set; }

        public PerformanceCounter TrainingDataRateCounter { get; set; }

        public PerformanceCounter ModelUploadFailedCounter { get; set; }

        public PerformanceCounter ExpUnitJsonErrorCounter { get; set; }

        public PerformanceCounter ReaderTimeoutCounter { get; set; }

        public PerformanceCounter CompleteUnitCounter { get; set; }

        public PerformanceCounter PositiveTrainingDataCounter { get; set; }

        public PerformanceCounter TrainingDataQueueCounter { get; set; }

        public PerformanceCounter DequeueTimeoutCounter { get; set; }

        public PerformanceCounter AvgVWTrainingTimeCounter { get; set; }

        public PerformanceCounter AvgVWTrainingTimeBaseCounter { get; set; }

        public PerformanceCounter AvgReaderEnumerateTimeCounter { get; set; }

        public PerformanceCounter AvgReaderEnumerateTimeBaseCounter { get; set; }

        public PerformanceCounter AvgModelUploadTimeCounter { get; set; }

        public PerformanceCounter AvgModelUploadTimeBaseCounter { get; set; }

        public long ReaderBatchProcessDataCount { get; set; }

        public long ReaderBatchProcessElapsed { get; set; }
    }
}
