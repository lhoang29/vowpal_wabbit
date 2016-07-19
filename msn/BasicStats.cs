using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace msn
{
    public static class BasicStats
    {

        public static void CostTable(List<Header> header)
        {
            // sum(cost / prob)
            var data = header
                .Select(d => d.Cost)
                .GroupBy(cost => cost)
                .Select(g => new { Cost = g.Key, Count = g.Count() })
                .OrderBy(g => g.Cost)
                .ToList();

            Console.WriteLine("Cost  Count");
            foreach (var d in data)
            {
                Console.WriteLine("{0,5} {1}", d.Cost, d.Count);
            }
            Console.WriteLine();
        }


        public static void EvalOfDefaultPolicy(List<Header> header, int policyAction = 1)
        {
            // sum(cost / prob)
            var data = header
                .Where(d => d.Action == policyAction)
                .Select(d => new { d.Cost, d.Probability })
                .ToList();

            var numerator = data.Sum(d => d.Cost / d.Probability);
            var denominator = data.Sum(d => 1 / d.Probability);

            Console.WriteLine("{0}: {1}", policyAction, numerator / denominator);
            Console.WriteLine();
        }

        public static void CountPerDocument(List<Header> header)
        {
            var query = header.Select(h => h.ChoosenDocId);

            var q2 = query.GroupBy(id => id)
                .Select(g => new { Id = g.Key, Count = g.Count() })
                .OrderByDescending(e => e.Count);

            Console.WriteLine("Count DocId");
            foreach (var item in q2)
            {
                Console.WriteLine("{0,5} {1}", item.Count, item.Id);
            }
            Console.WriteLine();
        }

        public static void Count(List<Header> header)
        {
            var query = header
                .GroupBy(d => d.Action)
                .Select(g => new { Action = g.Key, Count = g.Count(), AverageProb = g.Sum(d => d.Probability) / header.Count })
                .OrderBy(d => d.Action);

            var mQuery = query.ToList();

            //            var totalExamples = mQuery.Sum(d => d.Count);

            Console.WriteLine("Action  Count  Count/Total  sum(Prob)/Total");
            foreach (var d in mQuery)
            {
                Console.WriteLine("{0,2}: {1,5} {2:0.######} {3:0.######}", d.Action, d.Count, (float)d.Count / header.Count, d.AverageProb);
            }
            Console.WriteLine();
            Console.WriteLine("Crude Conf Int (1/sqrt(Total)): {0}", 1.0 / Math.Sqrt(header.Count));
            Console.WriteLine();
        }
    }
}
