namespace Microsoft.Content.Recommendations.TrainingRuntime
{
    using System.Text.RegularExpressions;

    internal static class Constants
    {
        internal static readonly Regex BlobNameRegex = new Regex(@"^(\d+)-(\d+)-([a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12})$", RegexOptions.Compiled);

        internal static readonly string PerfCounterCategory = "TrainingService";

        internal static readonly string ObservationDocIdKey = "i";

        internal static readonly string ObservationRewardKey = "r";

        internal static readonly char FeatureSeparator = '|';

        internal static readonly char EntrySeparator = ':';

        internal static readonly string TrainingDataIdPrefixFormat = "{0:D2}{1:D3}-";

        internal static readonly string ReaderContainerPrefix = "complete";

        internal static readonly string ContainerDateTimeFormat = "yyyyMMdd";

        internal static readonly string CompositeDocumentIdPrefix = "c-";

        internal static readonly int StreamMinimumReadSizeInBytes = 16 * 1024 * 1024;

        internal static readonly int DefaultTrainingDataQueueCount = 50000;

        internal static readonly int MaxCancellationTimeout = 15 * 1000;

        internal static readonly int MaxTakeTimeout = 3 * 60 * 1000;

        /// <summary>
        /// Trackback reload event name
        /// </summary>
        internal static readonly string TrackbackReloadEventName = "Reload";

        /// <summary>
        /// Trackback information event name
        /// </summary>
        internal static readonly string TrackbackInfoEventName = "I:";

        /// <summary>
        /// Max memory usage by trainer trackback manager
        /// </summary>
        internal static readonly long MaxTrackbackMemUsage = 1000 * 1024 * 1024;
    }
}
