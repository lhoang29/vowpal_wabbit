using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace msn_common
{
    public class SweepArguments
    {
        public string Arguments;

        public Random Random;

        public void InitializeRandom()
        {
            var seed = Arguments.GetHashCode();
            this.Random = new Random(seed);
        }
    }
}
