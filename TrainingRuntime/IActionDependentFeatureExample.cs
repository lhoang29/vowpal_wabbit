using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Content.Recommendations.TrainingRuntime
{
    public interface IActionDependentFeatureExample<T>
    {
        IReadOnlyList<T> ActionDependentFeatures { get; set; }
    }
}
