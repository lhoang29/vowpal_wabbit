namespace Microsoft.Content.Recommendations.TrainingRuntime.Reader
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Runtime.CompilerServices;
    using System.Text;
    using System.Text.RegularExpressions;
    using System.Threading;
    using System.Threading.Tasks;

    using Microsoft.Content.Recommendations.TrainingRuntime.Context;
    using Microsoft.Content.Recommendations.TrainingRuntime.RewardFunction;
    using Microsoft.Content.Recommendations.TrainingRuntime.Trainer;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using Newtonsoft.Json.Serialization;
    using VW;


    public class DecisionServiceReader<TContext, TActionDependentFeature> : IDataReader
        where TContext : class, IActionDependentFeatureExample<TActionDependentFeature>
        where TActionDependentFeature : IActionDependentFeature
    {
        private CloudStorageAccount storageAccount;

        private CloudBlobClient blobClient;
            
        private DateTime? startTimeInclusive = null;
        
        private DateTime? endTimeExclusive = null;

        private bool resumeFromTheEnd = false;

        private string applicationId;

        private int maxTrainingDataQueueCount;

        private IEnumerator<TrainingData> enumerator;

        private IList<BlobDetails> currentBlobs;

        private Dictionary<Guid, BlobDetails> resumeBlobs;

        private BlockingCollection<TrainingData> trainingDataQueue;

        private CancellationTokenSource loadCancellationTokenSource;

        private Task loadMasterTask;

        private CachingReferenceResolver referenceResolver;

        private Stopwatch perfStopwatch;
       

        public DecisionServiceReader(string appId, CloudStorageAccount storageAccount, int refCapacity, IRewardFunction rewardFunction, DateTime? startTimeInclusive = null, DateTime? endTimeExclusive = null, int maxTDQueueCount = 0)
        {
            this.applicationId = appId;
            this.maxTrainingDataQueueCount = (maxTDQueueCount == 0) ? Constants.DefaultTrainingDataQueueCount : maxTDQueueCount;
            
            this.storageAccount = storageAccount;
            if (storageAccount != null)
            {
                this.blobClient = this.storageAccount.CreateCloudBlobClient();
            }

            this.startTimeInclusive = startTimeInclusive;
            this.endTimeExclusive = endTimeExclusive;

            this.perfStopwatch = new Stopwatch();

            this.referenceResolver = new CachingReferenceResolver(refCapacity);

            this.Serializer = new JsonSerializer();
            this.Serializer.ReferenceResolver = this.referenceResolver;
            this.Serializer.Converters.Add(new DateTimeOffsetConverter());

            this.RewardFunction = rewardFunction;
        }

        public int CachedReferencesCount
        {
            get
            {
                return this.referenceResolver.Count;
            }
        }

        protected JsonSerializer Serializer { get; private set; }

        protected IRewardFunction RewardFunction { get; private set; }

        public void ResumeRead()
        {
            // check if resume blobs are up to date
            if (this.currentBlobs != null)
            {
                if (this.resumeBlobs == null)
                {
                    this.resumeBlobs = new Dictionary<Guid, BlobDetails>();
                }

                foreach (var blob in this.currentBlobs)
                {
                    // just in case, most likely not needed
                    if (blob.Index == BlobDetails.ReferenceIndex)
                    {
                        continue;
                    }

                    if (this.resumeBlobs.ContainsKey(blob.BlobGuid))
                    {
                        blob.ResumeOffset = blob.Length;
                        this.resumeBlobs[blob.BlobGuid] = blob;
                    }
                    else
                    {
                        blob.ResumeOffset = blob.Length;
                        this.resumeBlobs.Add(blob.BlobGuid, blob);
                    }
                }
            }

            // remove old blobs
            if (this.resumeBlobs != null && this.resumeBlobs.Count > 0)
            {
                var orderedBlobs = this.resumeBlobs.Values.OrderByDescending(t => t.BlobDateTime);
                var latestToken = orderedBlobs.First();

                if (orderedBlobs.Any(t => t.BlobDateTime != latestToken.BlobDateTime))
                {
                    this.resumeBlobs = this.resumeBlobs.Values.Where(t => t.BlobDateTime == latestToken.BlobDateTime).ToDictionary(x => x.BlobGuid);
                }
            }

            this.enumerator = this.LoadExperimentalUnits().GetEnumerator();
        }

        public void MoveToEnd()
        {
            this.resumeFromTheEnd = true;
        }

        public void Reset()
        {
            this.ResetLoadingTasks();

            this.resumeBlobs = null;
            this.enumerator = null;
        }

        /// <summary>
        /// Same semantics as IEnumerator.
        /// </summary>
        /// <returns>Returns true if an item was read, false if EOF was reached.</returns>
        public bool MoveNext()
        {
            if (this.enumerator == null)
            {
                this.enumerator = this.LoadExperimentalUnits().GetEnumerator();
            }

            return this.enumerator.MoveNext();
        }

        public TrainingData GetCurrentTrainingData()
        {
            return this.enumerator == null ? null : this.enumerator.Current; 
        }

        private IEnumerable<TrainingData> LoadExperimentalUnits()
        {
            this.ResetLoadingTasks();

            DateTime? startTime = this.startTimeInclusive;
            DateTime? endTime = this.endTimeExclusive;

            if (this.resumeBlobs != null && this.resumeBlobs.Count > 0)
            {
                startTime = this.resumeBlobs.First().Value.BlobDateTime;
            }

            this.currentBlobs = null;

            try
            {
                this.perfStopwatch.Restart();

                // list containers
                var containers = this.GetJoinedDataContainers(startTime, endTime);

                // list blobs
                this.currentBlobs = this.GetJoinedDataBlobs(containers, startTime, endTime);

                this.perfStopwatch.Stop();

                DiagnosticsStats.GetInstance().AvgReaderEnumerateTimeCounter.IncrementBy(this.perfStopwatch.ElapsedTicks);
                DiagnosticsStats.GetInstance().AvgReaderEnumerateTimeBaseCounter.Increment();
            }
            catch (Exception)
            {
                // problem with storage, retry next time
                yield break;
            }

            // initial with no start and end time
            if (this.resumeFromTheEnd || this.startTimeInclusive == null && this.endTimeExclusive == null && this.resumeBlobs == null)
            {
                this.resumeFromTheEnd = false;

                // move to the end of latest blobs
                var latestBlob = this.currentBlobs.OrderByDescending(blob => blob.BlobDateTime).FirstOrDefault();
                if (latestBlob != null)
                {
                    if (this.resumeBlobs == null)
                    {
                        this.resumeBlobs = new Dictionary<Guid, BlobDetails>();
                    }
                    else
                    {
                        this.resumeBlobs.Clear();
                    }

                    // latest blobs only
                    this.currentBlobs = this.currentBlobs.Where(blob => blob.BlobDateTime == latestBlob.BlobDateTime).ToList();
                    foreach (var b in this.currentBlobs)
                    {
                        // we need to initialize reference for this hour
                        if (b.Index == BlobDetails.ReferenceIndex)
                        {
                            continue;
                        }

                        b.ResumeOffset = b.Length;
                        this.resumeBlobs.Add(b.BlobGuid, b);
                    }
                }
            }

            // enumerate blob streams
            var blobStreams = this.GetBlobStreams(this.currentBlobs);
            var blobStreamsEnumerator = blobStreams.GetEnumerator();

            // no new data
            if (this.GetNextBlobStream(blobStreamsEnumerator) == null)
            {
                yield break;
            }

            // load training data from current blobs
            while (blobStreamsEnumerator != null)
            {
                this.loadCancellationTokenSource = new CancellationTokenSource();
                this.trainingDataQueue = new BlockingCollection<TrainingData>(this.maxTrainingDataQueueCount);

                var fetchTasks = new List<Task>();
                int currentHour = -1;

                // group by hour
                while (blobStreamsEnumerator != null)
                {
                    var currentBlobStream = blobStreamsEnumerator.Current;

                    if (currentHour == -1)
                    {
                        currentHour = currentBlobStream.Hour;
                    }
                    else if (currentHour != currentBlobStream.Hour)
                    {
                        break;
                    }

                    if (currentBlobStream.IsReference)
                    {
                        // load reference first
                        this.LoadTrainingData(currentBlobStream, this.trainingDataQueue, this.loadCancellationTokenSource.Token);
                    }
                    else
                    {
                        var task = Task.Run(() => { this.LoadTrainingData(currentBlobStream, this.trainingDataQueue, this.loadCancellationTokenSource.Token); });
                        fetchTasks.Add(task);
                    }

                    if (this.GetNextBlobStream(blobStreamsEnumerator) == null)
                    {
                        blobStreamsEnumerator = null;
                    }
                }

                if (fetchTasks.Count == 0)
                {
                    break;
                }

                this.loadMasterTask = Task.Run(() => this.LoadTrainingDataCompleted(fetchTasks, this.trainingDataQueue));

                while (!this.trainingDataQueue.IsCompleted)
                {
                    bool breakEnum = false;
                    TrainingData td = null;

                    try
                    {
                        if (this.trainingDataQueue.TryTake(out td, Constants.MaxTakeTimeout, this.loadCancellationTokenSource.Token))
                        {
                            DiagnosticsStats.GetInstance().TrainingDataQueueCounter.Decrement();
                        }
                        else
                        {
                            breakEnum = true;
                            DiagnosticsStats.GetInstance().DequeueTimeoutCounter.Increment();
                        }
                    }
                    catch (Exception ex)
                    {
                        if (ex is OperationCanceledException)
                        {
                            breakEnum = true;
                            Trace.TraceInformation("Dequeue training data canceled");
                        }
                        else if (ex is InvalidOperationException)
                        {
                            // if we are waiting for data then the loading tasks finished with no more data
                            breakEnum = true;
                        }
                        else
                        {
                            Trace.TraceError("Dequeue training data error: " + ex.Message);
                        }
                    }

                    if (breakEnum)
                    {
                        yield break;
                    }

                    yield return td;
                }
            }
        }

        private void LoadTrainingData(JoinDataStream dataStream, BlockingCollection<TrainingData> tdQueue, CancellationToken token)
        {
            bool isRef = dataStream.IsReference;
            var idPrefix = string.Format(Constants.TrainingDataIdPrefixFormat, dataStream.Hour, dataStream.Index);

            var units = DataReaderUtil.ParseExperimentalUnits(dataStream, this.Serializer, dataStream.Name);
            foreach (var unit in units)
            {
                // ignore cancellation request for reference stream
                if (token.IsCancellationRequested && !isRef)
                {
                    Trace.TraceWarning("LoadTrainingData canceled");
                    break;
                }

                if (unit == null)
                {
                    continue;
                }

                var td = DataReaderUtil.ParseTrainingData<TContext, TActionDependentFeature>(unit, isRef ? null : this.RewardFunction, this.Serializer, idPrefix);
                if (!isRef)
                {
                    try
                    {
                        tdQueue.Add(td, token);
                        DiagnosticsStats.GetInstance().TrainingDataQueueCounter.Increment();
                    }
                    catch (Exception ex)
                    {
                        if (ex is OperationCanceledException)
                        {
                            Trace.TraceInformation("Adding training data queue canceled");
                        }
                        else
                        {
                            Trace.TraceError("Adding training data queue error: " + ex.Message);
                        }

                        // stop loading
                        break;
                    }
                }
            }
        }

        private void LoadTrainingDataCompleted(IEnumerable<Task> loadTasks, BlockingCollection<TrainingData> tdQueue)
        {
            Task.WaitAll(loadTasks.ToArray());
            tdQueue.CompleteAdding();
        }

        private void ResetLoadingTasks()
        {
            try
            {
                if (this.loadCancellationTokenSource != null)
                {
                    this.loadCancellationTokenSource.Cancel();
                }

                if (this.loadMasterTask != null && !this.loadMasterTask.IsCompleted)
                {
                    if (!this.loadMasterTask.Wait(Constants.MaxCancellationTimeout))
                    {
                        Trace.TraceError("Failed to wait for master task complete");
                    }
                }

                if (this.trainingDataQueue != null)
                {
                    this.trainingDataQueue.Dispose();
                    this.trainingDataQueue = null;

                    DiagnosticsStats.GetInstance().TrainingDataQueueCounter.RawValue = 0;
                }
            }
            catch (Exception ex)
            {
                Trace.TraceError("ResetLoadingTasks: " + ex.Message);
            }

            this.loadCancellationTokenSource = null;
            this.loadMasterTask = null;
        }

        private JoinDataStream GetNextBlobStream(IEnumerator<JoinDataStream> enumerator)
        {
            if (enumerator == null)
            {
                return null;
            }

            while (enumerator.MoveNext())
            {
                var blobStream = enumerator.Current;
                if (blobStream != null)
                {
                    return blobStream;
                }
            }

            return null;
        }

        private IEnumerable<JoinDataStream> GetBlobStreams(IList<BlobDetails> blobs)
        {
            IEnumerable<BlobDetails> orderedBlobs = blobs.OrderBy(b => b, new BlobDetailsComparer());

            if (this.resumeBlobs == null)
            {
                this.resumeBlobs = new Dictionary<Guid, BlobDetails>();
            }

            if (this.resumeBlobs.Count > 0)
            {
                // return all resume blobs first
                foreach (var blob in orderedBlobs)
                {
                    if (!this.resumeBlobs.ContainsKey(blob.BlobGuid))
                    {
                        continue;
                    }

                    if (!blob.Blob.Exists())
                    {
                        continue;
                    }

                    JoinDataStream blobStream = null;

                    var reBlob = this.resumeBlobs[blob.BlobGuid];
                    if (reBlob.ResumeOffset > blob.Length)
                    {
                        // unexpected resume offset, do nothing and move to the end
                        blob.ResumeOffset = blob.Length;
                    }
                    if (reBlob.ResumeOffset == blob.Length)
                    {
                        // nothing new, don't do anything
                        blob.ResumeOffset = blob.Length;
                    }
                    else
                    {
                        // resume blob
                        blobStream = new JoinDataStream(blob.Blob, blob.BlobGuid, reBlob.ResumeOffset, blob.Length, blob.Index, blob.BlobDateTime.Hour);
                        blob.ResumeOffset = blobStream.ResumeOffset;
                    }

                    // update resume blobs
                    this.resumeBlobs[blob.BlobGuid] = blob;

                    // return stream
                    yield return blobStream;
                }

                orderedBlobs = orderedBlobs.Where(b => !this.resumeBlobs.ContainsKey(b.BlobGuid));
            }

            foreach (var blob in orderedBlobs)
            {
                if (!blob.Blob.Exists())
                {
                    continue;
                }

                // guid check just in case join server screw up
                if (this.resumeBlobs.ContainsKey(blob.BlobGuid))
                {
                    // blob should have unique id
                    continue;
                }

                var blobStream = new JoinDataStream(blob.Blob, blob.Length, blob.Index, blob.BlobDateTime.Hour);
                blob.ResumeOffset = blobStream.ResumeOffset;

                // new resume blobs
                this.resumeBlobs.Add(blob.BlobGuid, blob);

                // return stream
                yield return blobStream;
            }
        }

        private IList<ContainerDetails> GetJoinedDataContainers(DateTime? startTime, DateTime? endTime)
        {
            if (this.blobClient == null)
            {
                return null;
            }

            var containerPrefixBase = this.applicationId + Constants.ReaderContainerPrefix;
            string containerPrefix = null;
            bool isKnownContainer = false;

            if (startTime == null && endTime == null)
            {
                containerPrefix = string.Format("{0}{1:d4}{2:d2}", containerPrefixBase, DateTime.UtcNow.Year, DateTime.UtcNow.Month);
            }
            else if (startTime != null && startTime.Value.Date == DateTime.UtcNow.Date)
            {
                isKnownContainer = true;
            }
            else
            {
                containerPrefix = containerPrefixBase;
            }

            CloudBlobContainer latestContainer = null;
            DateTime latestContainerDateTime = DateTime.MinValue;

            var detailsList = new List<ContainerDetails>();

            if (isKnownContainer)
            {
                var currentContainerName = string.Format("{0}{1:d4}{2:d2}{3:d2}", containerPrefixBase, startTime.Value.Date.Year, startTime.Value.Date.Month, startTime.Value.Date.Day);
                latestContainer = this.blobClient.GetContainerReference(currentContainerName);
                latestContainerDateTime = startTime.Value.Date;
            }
            else
            {
                var containers = AzureStorageHelper.ListContainers(this.blobClient, containerPrefix);

                foreach (var container in containers)
                {
                    DateTime containerDateTime;

                    if (!DateTime.TryParseExact(container.Name.Substring(containerPrefixBase.Length, 8), Constants.ContainerDateTimeFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out containerDateTime))
                    {
                        continue;
                    }

                    if (startTime == null && endTime == null)
                    {
                        // always not valid to return only the latest container
                        if (latestContainer == null || latestContainerDateTime < containerDateTime)
                        {
                            latestContainer = container;
                            latestContainerDateTime = containerDateTime;
                        }
                    }
                    else
                    {
                        bool isValid = true;

                        if (startTime != null)
                        {
                            isValid = containerDateTime >= startTime.Value.Date;
                        }

                        if (isValid && endTime != null)
                        {
                            isValid = containerDateTime < endTime.Value.Date;
                        }

                        if (isValid)
                        {
                            detailsList.Add(new ContainerDetails()
                            {
                                Container = container,
                                DateTime = containerDateTime
                            });
                        }
                    }
                }
            }

            if (latestContainer != null)
            {
                detailsList.Add(new ContainerDetails()
                {
                    Container = latestContainer,
                    DateTime = latestContainerDateTime
                });
            }

            return detailsList;
        }

        private IList<BlobDetails> GetJoinedDataBlobs(IList<ContainerDetails> containers, DateTime? startTime, DateTime? endTime)
        {
            var detailsList = new List<BlobDetails>();

            if (containers == null)
            {
                return detailsList;
            }

            foreach (var container in containers)
            {
                var blobs = AzureStorageHelper.ListBlobs(container.Container);

                foreach (var blob in blobs)
                {
                    if (blob.GetType() != typeof(CloudBlockBlob))
                    {
                        continue;
                    }

                    var b = (CloudBlockBlob)blob;
                    var match = Constants.BlobNameRegex.Match(b.Name);

                    if (match.Success && match.Groups.Count >= 4)
                    {
                        bool isValid = true;

                        var hour = int.Parse(match.Groups[1].Value);
                        var index = int.Parse(match.Groups[2].Value);
                        var blobGuid = Guid.Parse(match.Groups[3].Value);

                        var blobDetails = new BlobDetails(b, container.DateTime, hour, index, blobGuid);

                        if (startTime != null)
                        {
                            isValid = blobDetails.BlobDateTime >= startTime;
                        }

                        if (isValid && endTime != null)
                        {
                            isValid = blobDetails.BlobDateTime < endTime;
                        }

                        if (isValid)
                        {
                            detailsList.Add(blobDetails);
                        }
                    }
                }
            }

            return detailsList;
        }
    }
}
