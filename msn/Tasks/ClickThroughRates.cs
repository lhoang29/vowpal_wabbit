using Microsoft.Content.Recommendations.TrainingRuntime.Context;
using Microsoft.Research.MultiWorldTesting.ExploreLibrary;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace msn.Tasks
{
  public static class ClickThroughRates
  {
    private class LocalHeader
    {
      internal int Action;

      internal float Cost;

      internal float Probability;
    }

    private class LocalStats
    {
      internal int Chosen;

      internal int Clicks;

      internal float Expected;

      internal float Marginal;
    }

    public static void ExportHeaders(string inputDirectory, string prefix, float epsilon, DateTime? startTimeInclusive = null, DateTime? endTimeExclusive = null)
    {
      var explore = new List<LocalHeader>();
      var exploit = new List<LocalHeader>();

      using (var writer = new StreamWriter(string.Format("{0}\\{1}.header.{2:yyyyMMdd}.csv", inputDirectory, prefix, startTimeInclusive)))
      {
        writer.WriteLine("action\tcost\tprob\ttype");
        var dataSet = Data.Read(inputDirectory, prefix, startTimeInclusive, endTimeExclusive).Where(d => d != null);
        float eps_total = 0f;
        int transfer_tot = 0;
        foreach (var data in dataSet)
        {
          var d = (UserContext<DocumentFeatures>)data.Context;

          uint numActions = (uint)d.ActionDependentFeatures.Count;

          // data.Id == uniqueKey
          // appId == authorizationToken
          //string.Empty; //
          var authorizationToken = "53ea9a7a-1f26-4d45-8ff5-8f6a5aa36ba9";
          var uniqueKey = data.Id.Substring(data.Id.Length - 36);
          var saltedSeed = MurMurHash3.ComputeIdHash(uniqueKey) + MurMurHash3.ComputeIdHash(authorizationToken);
          var random = new PRG(saltedSeed);

          // t ... exploit
          // e ... explore
          char type;
          float outProbability;
          float baseProbability = epsilon / numActions; // uniform probability
          if (random.UniformUnitInterval() < 1f - epsilon)
          {
            type = 't';
            outProbability = 1f - epsilon + baseProbability;
          }
          else
          {
            type = 'e';

            // Get uniform random 1-based action ID
            uint actionId = (uint)random.UniformInt(1, numActions);

            if (data.Probability > epsilon)
            {
              outProbability = 1f - epsilon + baseProbability;
            }
            else
            {
              // Otherwise it's just the uniform probability
              outProbability = baseProbability;
            }

            if (actionId != data.Action)
            {
              Console.WriteLine("divergence");
            }
          }

          if (Math.Abs(Math.Abs(data.Probability) - Math.Abs(outProbability)) > 1e-4)
          {
            Console.WriteLine("divergence");
          }

          // Johns method
          if (data.Probability < 1 - epsilon)
            type = 'e';
          else
          {
            float l_epsilon = data.Probability - (1 - epsilon);
            eps_total += l_epsilon / data.Probability;
            if (Math.Floor(eps_total) > transfer_tot)
            {
              type = 'e';
              transfer_tot++;
            }
            else
            {
              type = 't';
            }
          }

          (type == 'e' ? explore : exploit).Add(new LocalHeader
          {
            Action = data.Action,
            Cost = data.Cost,
            Probability = data.Probability
          });

          writer.WriteLine("{0}\t{1}\t{2}\t{3}", data.Action, data.Cost, data.Probability, type);
        }

        var exploreStats = Stats(explore);
        var exploitStats = Stats(exploit);

        Console.WriteLine("PERFORMANCE");
        Console.WriteLine("Model:     {0}", exploitStats.Marginal);
        Console.WriteLine("Editorial: {0}", Stats(explore.Where(h => h.Action == 1)).Marginal);
        Console.WriteLine();

        Console.WriteLine("Explore");
        StatsByAction(explore);
        Console.WriteLine("Exploit");
        StatsByAction(exploit);

        Console.WriteLine("SUMMARY");
        Console.WriteLine("        {0,-10} {1,-10} {2,-10}", "clicks", "chosen", "marginal");
        Console.WriteLine("explore {0,-10} {1,-10} {2}", exploreStats.Clicks, exploreStats.Chosen, exploreStats.Marginal);
        Console.WriteLine("exploit {0,-10} {1,-10} {2}", exploitStats.Clicks, exploitStats.Chosen, exploitStats.Marginal);
      }
    }

    private static void StatsByAction(List<LocalHeader> data)
    {
      var byAction = data.GroupBy(h => h.Action, (action,group) => new { action, stats = Stats(group) });

      Console.WriteLine("#  {0,-10} {1,-10} {2,-10} {3,-10}", "chosen","clicks", "expected" ,"marginals");
      foreach (var stats in byAction.OrderBy(g => g.action))
      {
        Console.WriteLine("{0,-2} {1,-10} {2,-10} {3:0.0000000} {4:0.0000000}",
          stats.action,
          stats.stats.Chosen,
          stats.stats.Clicks,
          stats.stats.Expected,
          stats.stats.Marginal);
      }
      Console.WriteLine();
    }

    private static LocalStats Stats(IEnumerable<LocalHeader> data)
    {
      // materialize
      data = data.ToArray();

      // chosen = nrow(group)
      int chosen = data.Count();

      //expected = sum(group$cost * (1 / group$prob)) / sum(1 / group$prob)
      float expected = data.Sum(h => h.Cost * (1 / h.Probability)) / data.Sum(h => 1 / h.Probability);

      //clicks = sum(group$cost == -1)
      int clicks = data.Count(h => h.Cost == -1);

      // marginal = clicks / chosen
      var marginal = clicks / (float)chosen;

      return new LocalStats
      {
        Chosen = chosen,
        Expected = expected * -1,
        Clicks = clicks,
        Marginal = marginal
      };
    }
  }
}
