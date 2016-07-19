namespace Microsoft.Content.Recommendations.TrainingRuntime.Reader
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    using Microsoft.Content.Recommendations.TrainingRuntime.Context;
    using Microsoft.Content.Recommendations.TrainingRuntime.RewardFunction;
    using Microsoft.Content.Recommendations.TrainingRuntime.Trainer;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using VW;


    public class FileDataReader<TContext, TActionDependentFeature> : IDataReader
        where TContext : class, IActionDependentFeatureExample<TActionDependentFeature>
        where TActionDependentFeature : IActionDependentFeature
    {
        private string rootPath;

        private string folderPrefix;

        private string currentFolder;

        private DateTime? startTimeInclusive = null;

        private DateTime? endTimeExclusive = null;

        private ExperimentalUnit currentUnit;

        private IEnumerator<TrainingData> enumerator;

        private CachingReferenceResolver referenceResolver;

        public FileDataReader(string folderPath, int refCapacity, IRewardFunction rewardFunction)
        {
            if (folderPath.Last() == Path.DirectorySeparatorChar)
            {
                folderPath = folderPath.Substring(0, folderPath.Length - 1);
            }

            var sepIdx = folderPath.LastIndexOf(Path.DirectorySeparatorChar);

            this.rootPath = (sepIdx == -1) ? "." : folderPath.Substring(0, sepIdx);

            this.currentFolder = (sepIdx == -1) ? folderPath : folderPath.Substring(sepIdx + 1);

            this.Initialize(refCapacity, rewardFunction);
        }

        public FileDataReader(string rootPath, string folderPrefix, int refCapacity, IRewardFunction rewardFunction, DateTime? startTimeInclusive = null, DateTime? endTimeExclusive = null)
        {
            if (string.IsNullOrEmpty(rootPath))
            {
                this.rootPath = ".";
            }
            else
            {
                this.rootPath = (rootPath.Last() == Path.DirectorySeparatorChar) ? rootPath.Substring(0, rootPath.Length - 1) : rootPath;
            }

            this.folderPrefix = folderPrefix;

            this.startTimeInclusive = startTimeInclusive;

            this.endTimeExclusive = endTimeExclusive;

            this.Initialize(refCapacity, rewardFunction);
        }

        public JsonSerializer Serializer { get; private set; }


        public int CachedReferencesCount
        {
            get
            {
                return this.referenceResolver.Count;
            }
        }

        protected IRewardFunction RewardFunction { get; private set; }

        public void Reset()
        {
            throw new NotImplementedException();
        }

        public void ResumeRead()
        {
        }

        public void MoveToEnd()
        {
            throw new NotImplementedException();
        }

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
            if (this.enumerator != null)
            {
                return this.enumerator.Current;
            }

            return null;
        }

        public ExperimentalUnit GetCurrentUnitData()
        {
            return this.currentUnit;
        }

        private void Initialize(int refCapacity, IRewardFunction rewardFunction)
        {
            this.referenceResolver = new CachingReferenceResolver(refCapacity);

            this.Serializer = new JsonSerializer();
            this.Serializer.ReferenceResolver = this.referenceResolver;

            this.RewardFunction = rewardFunction;
        }

        private IEnumerable<TrainingData> LoadExperimentalUnits()
        {
            List<DateTime> foldersDateTime = new List<DateTime>();

            if (!Directory.Exists(this.rootPath))
            {
                throw new ArgumentException("Root path not found");
            }

            if (string.IsNullOrEmpty(this.folderPrefix))
            {
                DateTime containerDateTime;
                if (!DateTime.TryParseExact(this.currentFolder.Substring(this.currentFolder.Length - 8, 8), Constants.ContainerDateTimeFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out containerDateTime))
                {
                    throw new ArgumentException("Invalid folder name (must end with date)");
                }

                this.folderPrefix = this.currentFolder.Substring(0, this.currentFolder.Length - 8);
                foldersDateTime.Add(containerDateTime);
            }
            else
            {
                var folders = Directory.EnumerateDirectories(this.rootPath);

                foreach (var folder in folders)
                {
                    var folderName = folder;
                    var folderSepIdx = folder.LastIndexOf(Path.DirectorySeparatorChar);
                    if (folderSepIdx != -1)
                    {
                        folderName = folder.Substring(folderSepIdx + 1);
                    }

                    if (!folderName.StartsWith(this.folderPrefix, StringComparison.OrdinalIgnoreCase))
                    {
                        continue;
                    }

                    DateTime containerDateTime;
                    if (!DateTime.TryParseExact(folderName.Substring(folderName.Length - 8, 8), Constants.ContainerDateTimeFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out containerDateTime))
                    {
                        throw new ArgumentException("Invalid folder name (must end with date)");
                    }

                    if (startTimeInclusive != null && containerDateTime < startTimeInclusive.Value.Date)
                    {
                        continue;
                    }

                    if (endTimeExclusive != null && containerDateTime >= endTimeExclusive.Value)
                    {
                        continue;
                    }

                    foldersDateTime.Add(containerDateTime);
                }
            }

            if (foldersDateTime.Count == 0)
            {
                return new TrainingData[0];
                //yield break;
            }

            List<BlobDetails> fileDetails = new List<BlobDetails>();

            foreach (var folderDateTime in foldersDateTime.OrderBy(d => d))
            {
                var folderPath = string.Format("{0}\\{1}{2}", this.rootPath, this.folderPrefix, folderDateTime.ToString(Constants.ContainerDateTimeFormat));

                var files = Directory.EnumerateFiles(folderPath);
                foreach (var file in files)
                {
                    var filename = file;
                    var folderSepIdx = file.LastIndexOf(Path.DirectorySeparatorChar);
                    if (folderSepIdx != -1)
                    {
                        filename = file.Substring(folderSepIdx + 1);
                    }

                    var match = Constants.BlobNameRegex.Match(filename);
                    if (match.Success && match.Groups.Count >= 4)
                    {
                        bool isValid = true;

                        var hour = int.Parse(match.Groups[1].Value);
                        var index = int.Parse(match.Groups[2].Value);
                        var blobGuid = Guid.Parse(match.Groups[3].Value);

                        var blobDetails = new BlobDetails(file, folderDateTime, hour, index, blobGuid);

                        if (startTimeInclusive != null)
                        {
                            isValid = blobDetails.BlobDateTime >= startTimeInclusive.Value;
                        }

                        if (isValid && endTimeExclusive != null)
                        {
                            isValid = blobDetails.BlobDateTime < endTimeExclusive.Value;
                        }

                        if (isValid)
                        {
                            fileDetails.Add(blobDetails);
                        }
                    }
                }
            }

            var orderedFileDetails = fileDetails.OrderBy(b => b, new BlobDetailsComparer());

            Parallel.ForEach(
                orderedFileDetails.Where(fileDetail => fileDetail != null && fileDetail.Index == BlobDetails.ReferenceIndex),
                new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount },
                referenceData => {
                    var units = DataReaderUtil.ParseExperimentalUnits(File.OpenRead(referenceData.Filename), this.Serializer);
                    foreach (var unit in units)
                    {
                        DataReaderUtil.ParseTrainingData<TContext, TActionDependentFeature>(unit, null, this.Serializer);
                    }
                });

            var groupByHour = orderedFileDetails
                .Where(fileDetail => fileDetail != null && fileDetail.Index != BlobDetails.ReferenceIndex)
                .GroupBy(fileDetail => fileDetail.BlobDateTime);

            object lockObj = new object();
            ParallelQuery<TrainingData> orderedByHour = null;
            foreach (var g in groupByHour)
            {
                // parse each hour in parallel
                var deserialized = g.AsParallel()
                    .Select(fileDetail =>
                    {
                        lock (lockObj)
                        {
                            Console.WriteLine("Parsing: " + fileDetail.Filename);
                        }

                        return new
                        {
                            idPrefix = string.Format(Constants.TrainingDataIdPrefixFormat, fileDetail.BlobDateTime.Hour, fileDetail.Index),
                            units = DataReaderUtil.ParseExperimentalUnits(File.OpenRead(fileDetail.Filename), this.Serializer)
                        };
                    })
                    .Where(u => u.units != null)
                    .SelectMany(u => u.units.Select(unit => DataReaderUtil.ParseTrainingData<TContext, TActionDependentFeature>(unit, this.RewardFunction, this.Serializer, u.idPrefix)));

                orderedByHour = orderedByHour == null ? deserialized.WithDegreeOfParallelism(Environment.ProcessorCount) : orderedByHour.Concat(deserialized);
            }

            return orderedByHour;

            //foreach (var fileDetail in orderedFileDetails)
            //{
            //    var idPrefix = string.Format(Constants.TrainingDataIdPrefixFormat, fileDetail.BlobDateTime.Hour, fileDetail.Index);
            //    var units = DataReaderUtil.ParseExperimentalUnits(File.OpenRead(fileDetail.Filename), this.Serializer);
            //    if (units != null)
            //    {
            //        if (fileDetail.Index == BlobDetails.ReferenceIndex)
            //        {
            //            foreach (var unit in units)
            //            {
            //                DataReaderUtil.ParseTrainingData<TContext, TActionDependentFeature>(unit, null, this.Serializer);
            //            }
            //        }
            //        else
            //        {
            //            foreach (var unit in units)
            //            {
            //                if (unit != null)
            //                {
            //                    this.currentUnit = unit;
            //                    yield return DataReaderUtil.ParseTrainingData<TContext, TActionDependentFeature>(unit, this.RewardFunction, this.Serializer, idPrefix);
            //                }
            //            }
            //        }
            //    }
            //}
        }
    }
}
