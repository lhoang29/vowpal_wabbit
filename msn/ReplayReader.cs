using Microsoft.Content.Recommendations.TrainingRuntime.Trainer;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace msn
{
    public class ReplayReader
    {
        public static void Read(string inputDirectory, string prefix, string modelPrefix, DateTime? startTimeInclusive, DateTime? endTimeExclusive,
            Action<string> restart, Action<string> reload, Action<DateTime, string, TrainingData> dataFunc,
            uint trainInstance = 0, uint readAheadMax = 100000,
            Action<string, string> missing = null, Action<string> foundLater = null,
            Action<DateTime, string, string> endOfTrackback = null,
            Func<bool> stop = null)
        {
            var trackback = new List<FinalOutput>();
            for (DateTime currentDay = startTimeInclusive.Value; currentDay < endTimeExclusive.Value; currentDay = currentDay.AddDays(1))
            {
                var modelDir = string.Format(@"{0}\{1}-{2:yyyyMMdd}", inputDirectory, modelPrefix, currentDay);

            Console.WriteLine("Reading trackback from {0}", modelDir);
            var trackbackQuery =
                from f in Directory.EnumerateFiles(modelDir)
                let match = Regex.Match(Path.GetFileName(f), @"^model-(\d+)-(\d+)(.trackback(?:.deployed)?)?$")
                where match.Success
                    let output = new IntermediateOutput
                {
                    Filename = f,
                    Instance = int.Parse(match.Groups[1].Value),
                    Timestamp = new DateTime(long.Parse(match.Groups[2].Value)),
                    IsTrackback = match.Groups[3].Success,
                    IsDeployed = match.Groups[3].Value.EndsWith("deployed")
                }
                    where output.Instance == trainInstance // filter 2-instance
                group output by output.Timestamp into g
                    let output = new FinalOutput
                {
                    Timestamp = g.Key,
                    Model = g.FirstOrDefault(a => !a.IsTrackback),
                    Trackback = g.FirstOrDefault(a => a.IsTrackback),
                    Files = g.ToArray()
                }
                where output.Model != null && output.Trackback != null
                orderby output.Timestamp
                select output;

                trackback.AddRange(trackbackQuery.ToList());
            }


            var readAheadCache = new Dictionary<string, TrainingData>();
            var missingDict = new Dictionary<string, string>();

            var datasetEnumerator = Data.Read(inputDirectory, prefix, startTimeInclusive, endTimeExclusive)
                .Where(t => t != null)
                .GetEnumerator();

            int exCount = 0;
            foreach (var block in trackback)
            {
                var trackbackData = File.ReadAllLines(block.Trackback.Filename);

                // the model name listed at the beginning of the file
                string startingModelFileName = null;

                // early check for missing events
                // if not enough events found, model cannot be reproduced
                bool missingEvents = false;

                for (int i = 0; i < trackbackData.Length; i++)
                {
                    var line = trackbackData[i];

                    if (line.StartsWith("I: "))
                    {
                        line = trackbackData[++i];
                        if (!line.StartsWith("model-"))
                        {
                            Console.WriteLine("Expected model after init line!");
                            continue;
                        }

                        restart(Path.Combine(Path.GetDirectoryName(block.Trackback.Filename), line));

                        // If there's already a missing event, no point reading further unless
                        // a restart or reload appears, because model is no longer reproducible
                        // Since model is restarted, reset missingEvents to read further
                        missingEvents = false;

                        continue;
                    }

                    if (line.StartsWith("model-"))
                    {
                        startingModelFileName = Path.Combine(Path.GetDirectoryName(block.Trackback.Filename), line);
                        continue;
                    }

                    if (line == "Reload")
                    {
                        reload(startingModelFileName);

                        // If there's already a missing event, no point reading further unless
                        // a restart or reload appears, because model is no longer reproducible
                        // Since model is reloaded, reset missingEvents to read further
                        missingEvents = false;

                        continue;
                    }

                    if (line.Contains("RF:ClickReward"))
                    {
                        // ignore reward function line
                        continue;
                    }

                    // If there's already a missing event, no point reading further unless
                    // a restart or reload appears, because model is no longer reproducible
                    if (missingEvents)
                    {
                        continue;
                    }

                    if (stop != null && stop())
                    {
                        return;
                    }

                    var eventId = line;

                    TrainingData data;
                    if (readAheadCache.TryGetValue(eventId, out data))
                    {
                        dataFunc(block.Timestamp, block.Model.Filename, data);
                        // TODO: re-enable me
                        readAheadCache.Remove(eventId);
                    }
                    else
                    {
                        var readAheadCount = 0;
                        //if (readAheadCache.Count > 10000)
                        //{
                        //    readAheadMax = 100;
                        //}
                        while (datasetEnumerator.MoveNext())
                        {
                            data = datasetEnumerator.Current;

                            if (stop != null && stop())
                            {
                                return;
                            }

                            if (data.Id == eventId)
                            {
                                dataFunc(block.Timestamp, block.Model.Filename, data);
                                readAheadCache.Remove(eventId);
                                break;
                            }
                            else
                            {
                                readAheadCache.Add(data.Id, data);
                                readAheadCount++;

                                if (readAheadCount > readAheadMax)
                                {
                                    //if (!missingDict.ContainsKey(eventId))
                                    //{
                                    //    missingDict.Add(eventId, block.Model.Filename);
                                    //}

                                    if (missing != null)
                                    {
                                        Console.WriteLine("MISSING EVENT {0} FROM FILE {1}, READ AHEAD {2}",
                                            eventId, Path.GetFileName(block.Trackback.Filename), readAheadCount);

                                        // notify of missing events
                                        missing(eventId, Path.GetFileName(block.Trackback.Filename));

                                        missingEvents = true;
                                    }
                                    break;
                                }
                            }
                        }
                    }
                    //if (exCount % 10000 == 0)
                    //{
                    //    Console.WriteLine("read ahead cache size: " + readAheadCache.Count);
                    //    exCount = 0;
                    //    }
                    exCount++;
                    }

                if (missingEvents)
                {
                    continue;
                }

                // only call back if all events were found
                if (endOfTrackback != null)
                    endOfTrackback(block.Timestamp, startingModelFileName, block.Model.Filename);
            }
        }
            }

    public class IntermediateOutput
    {
        public string Filename { get; set; }
        public int Instance { get; set; }
        public DateTime Timestamp { get; set; }
        public bool IsTrackback { get; set; }
        public bool IsDeployed { get; set; }
        }

    public class FinalOutput
    {
        public DateTime Timestamp { get; set; }
        public IntermediateOutput Model { get; set; }
        public IntermediateOutput Trackback { get; set; }
        public IntermediateOutput[] Files { get; set; }
    }
}
