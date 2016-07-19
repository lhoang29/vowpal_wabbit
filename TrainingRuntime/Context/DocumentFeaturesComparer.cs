namespace Microsoft.Content.Recommendations.TrainingRuntime.Context
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// DocumentFeatures equality comparer
    /// </summary>
    public class DocumentFeaturesComparer : IEqualityComparer<DocumentFeatures>
    {
        public bool Equals(DocumentFeatures x, DocumentFeatures y)
        {
            if (x == null || y == null)
            {
                return (x == null && y == null);
            }

            return x.Equals(y);
        }

        public int GetHashCode(DocumentFeatures df)
        {
            return df.GetHashCode();
        }
    }
}
