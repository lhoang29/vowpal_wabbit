namespace Microsoft.Content.Recommendations.TrainingRuntime.Reader
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
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
    using VW.Labels;


    public static class DataReaderUtil
    {
        public static IEnumerable<ExperimentalUnit> ParseExperimentalUnits(Stream stream, JsonSerializer serializer = null, string streamId = null)
        {
            if (stream == null)
            {
                yield break;
            }

            try
            {
                var jsonReader = new JsonTextReader(new StreamReader(stream));
                jsonReader.DateParseHandling = DateParseHandling.None;

                if (serializer == null)
                {
                    serializer = new JsonSerializer();
                }

                //  Header: {
                if (!jsonReader.Read())
                {
                    yield break;
                }

                if (jsonReader.TokenType != JsonToken.StartObject)
                {
                    Trace.TraceError(string.Format("ParseExperimentalUnits: [{0}] unexpected token", (streamId == null) ? string.Empty : streamId));
                    yield break;
                }

                // Header "blob": 
                if (!jsonReader.Read())
                {
                    Trace.TraceError(string.Format("ParseExperimentalUnits: [{0}] unexpected EOF", (streamId == null) ? string.Empty : streamId));
                    yield break;
                }

                if (jsonReader.TokenType != JsonToken.PropertyName)
                {
                    Trace.TraceError(string.Format("ParseExperimentalUnits: [{0}] unexpected token", (streamId == null) ? string.Empty : streamId));
                    yield break;
                }

                var propertyName = Convert.ToString(jsonReader.Value);
                if (propertyName != "blob")
                {
                    Trace.TraceError(string.Format("ParseExperimentalUnits: [{0}] invalid header - {1}", (streamId == null) ? string.Empty : streamId, propertyName));
                    yield break;
                }

                // Header blobId value
                if (!jsonReader.Read())
                {
                    Trace.TraceError(string.Format("ParseExperimentalUnits: [{0}] unexpected EOF", (streamId == null) ? string.Empty : streamId));
                    yield break;
                }

                // Header "data": 
                if (!jsonReader.Read())
                {
                    Trace.TraceError(string.Format("ParseExperimentalUnits: [{0}] unexpected EOF", (streamId == null) ? string.Empty : streamId));
                    yield break;
                }

                propertyName = Convert.ToString(jsonReader.Value);
                if (propertyName != "data")
                {
                    Trace.TraceError(string.Format("ParseExperimentalUnits: [{0}] invalid header - {1}", (streamId == null) ? string.Empty : streamId, propertyName));
                    yield break;
                }

                // Actual data
                while (jsonReader.Read())
                {
                    switch (jsonReader.TokenType)
                    {
                        case JsonToken.StartArray:
                            // consume it and move on
                            if (!jsonReader.Read())
                            {
                                // EOF
                                yield break;
                            }

                            switch (jsonReader.TokenType)
                            {
                                case JsonToken.StartObject:
                                    // new data
                                    break;
                                case JsonToken.EndArray:
                                    // EOF
                                    yield break;
                                default:
                                    Trace.TraceError(string.Format("ParseExperimentalUnits: [{0}] unexpected token while parsing data array", (streamId == null) ? string.Empty : streamId));
                                    yield break;
                            }

                            break;

                        case JsonToken.EndArray:
                            // EOF
                            yield break;
                    }

                    ExperimentalUnit expUnit = null;

                    try
                    {
                        expUnit = serializer.Deserialize<ExperimentalUnit>(jsonReader);
                    }
                    catch (Exception ex)
                    {
                        // JSON serializer exception: malformed JSON so we are giving up current stream.
                        var blobPrefix = string.Format("{0}: ", (streamId == null) ? string.Empty : streamId);
                        Trace.TraceError(blobPrefix + ex.ToString());
                        DiagnosticsStats.GetInstance().ExpUnitJsonErrorCounter.Increment();
                        yield break;
                    }

                    yield return expUnit;
                }
            }
            finally
            {
                // need to dispose here to work with deferred execution
                stream.Dispose();
            }
        }

        public static TrainingData ParseTrainingData<TContext, TActionDependentFeature>(ExperimentalUnit unit, IRewardFunction rewardFunction, JsonSerializer serializer = null, string prefix = null)
            where TContext : class, IActionDependentFeatureExample<TActionDependentFeature>
            where TActionDependentFeature : IActionDependentFeature
        {
            if (unit.Interactions == null)
            {
                // ignore this unit if interactions are missing
                return null;
            }

            var interaction = unit.Interactions.FirstOrDefault(i => i.IsMultiAction && i.Actions != null && i.Actions.Any());
            if (interaction == null)
            {
                // ignore this unit
                return null;
            }

            double cost = double.NaN;
            TContext context = null;

            // deserialize contexts
            if (interaction.Context != null)
            {
                if (serializer == null)
                {
                    serializer = new JsonSerializer();
                }

                try
                {
                    context = (TContext)interaction.Context.ToObject(typeof(TContext), serializer);
                }
                catch (Exception ex)
                {
                    Trace.TraceWarning("Error parsing context data for unit {0}: {1}", unit.Id, ex.Message);
                }
            }

            if (context == null || context.ActionDependentFeatures == null || context.ActionDependentFeatures.Count == 0 || rewardFunction == null)
            {
                // no features in unit, ignore
                return null;
            }

            float probability = Convert.ToSingle(interaction.Probability);
            if (probability == 0.0)
            {
                // bad data
                return null;
            }

            if (context.ActionDependentFeatures.Any(action => action == null))
            {
                // failed to find reference for a document features
                DiagnosticsStats.GetInstance().UnresolvedRefCounter.Increment();
                return null;
            }

            // TODO: workaround for null document features
            if (typeof(TContext) == typeof(UserContext<DocumentFeatures>))
            {
                var userCtx = context as UserContext<DocumentFeatures>;
                if (userCtx.User == null)
                {
                    // create dummy user features
                    userCtx.User = new UserFeatures();
                }

                // check whether we need to create dummy user vector
                if (userCtx.UserLDAVector == null || userCtx.UserLDAVector.Vectors == null)
                {
                    var docFeature = userCtx.ActionDependentFeatures.FirstOrDefault(x => x.LDAVector != null && x.LDAVector.Vectors != null);
                    if (docFeature == null)
                    {
                        return null;
                    }

                    userCtx.UserLDAVector = new LDAFeatureVector(docFeature.LDAVector.Vectors.Length);
                }

                var docVectorLength = userCtx.UserLDAVector.Vectors.Length;

                foreach (var adf in userCtx.ActionDependentFeatures)
                {
                    if (adf.LDAVector == null)
                    {
                        adf.LDAVector = new LDAFeatureVector(docVectorLength);
                    }
                }
                // end of workaround
            }

            var observationToken = (unit.Observations != null && unit.Observations.Any()) ? unit.Observations.First() : null;

            // reward
            if (observationToken == null)
            {
                cost = rewardFunction.GetNoObservationCost(context, interaction.Actions, probability);
            }
            else
            {
                int clickedAction = -1;
                string clickedDocId = (string)observationToken[Constants.ObservationDocIdKey];
                var reward = (double)observationToken[Constants.ObservationRewardKey];

                if (!string.IsNullOrWhiteSpace(clickedDocId))
                {
                    for (var idx = 0; idx < context.ActionDependentFeatures.Count; idx++)
                    {
                        var docId = context.ActionDependentFeatures[idx].Id;
                        var doPartialMatch = docId.StartsWith(Constants.CompositeDocumentIdPrefix);

                        if (doPartialMatch)
                        {
                            if (!docId.Contains(clickedDocId))
                            {
                                continue;
                            }
                        }
                        else
                        {
                            if (!clickedDocId.Equals(docId))
                            {
                                continue;
                            }
                        }

                        // action is 1-based
                        clickedAction = idx + 1;
                        break;
                    }
                }

                cost = rewardFunction.GetCost(context, interaction.Actions, probability, clickedAction, reward);
                if (cost < 0)
                {
                    DiagnosticsStats.GetInstance().PositiveTrainingDataCounter.Increment();
                }

                DiagnosticsStats.GetInstance().CompleteUnitCounter.Increment();
            }

            // invalid cost, skip this unit
            if (double.IsNaN(cost))
            {
                return null;
            }

            // top action check
            var topAction = interaction.Actions[0];
            var topActionIdx = topAction - 1;
            if (topActionIdx < 0 || topActionIdx >= context.ActionDependentFeatures.Count)
            {
                return null;
            }

            var eventId = unit.Id;
            if (!string.IsNullOrEmpty(prefix))
            {
                eventId = string.Concat(prefix, eventId);
            }

            // new training data from this unit
            return new TrainingData() { Id = eventId, Action = topAction, Probability = probability, Cost = (float)cost, Context = context };
        }
    }
}
