using Microsoft.Content.Recommendations.TrainingRuntime.Context;
using Newtonsoft.Json.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace msn
{
    public class OutputCachingReferenceResolver : IReferenceResolver
    {
        private readonly Dictionary<string, object> references;
        private readonly Dictionary<object, string> reverseReferences;
        //private readonly ReaderWriterLockSlim rwLock = new ReaderWriterLockSlim();

        public OutputCachingReferenceResolver(Dictionary<string, object> references = null)
        {
            this.references = references ?? new Dictionary<string, object>();
            this.reverseReferences = new Dictionary<object, string>(new ReferenceEqualComparer());
        }

        public Dictionary<string, object> OutputDictionary
        {
            get
            {
                lock (this.reverseReferences)
                {
                    return reverseReferences.Where(kv => kv.Key != null && kv.Value != null)
                        .GroupBy(g => g.Value, g => g.Key)
                        .ToDictionary(g => g.Key, g => g.First());
                }
            }
        }

        public void AddReference(object context, string reference, object value)
        {
            this.references[reference] = value;
        }

        public object ResolveReference(object context, string reference)
        {
            object item;
            if (this.references.TryGetValue(reference, out item))
            {
                return item;
            }

            return null;
        }

        public string GetReference(object context, object value)
        {
            //try
            //{
            //    rwLock.EnterUpgradeableReadLock();

                string reference;
                if (!this.reverseReferences.TryGetValue(value, out reference))
                {
                    reference = (this.reverseReferences.Count + 1).ToString();
                    //try
                    //{
                    //    rwLock.EnterWriteLock();
                        this.reverseReferences[value] = reference;
                    //}
                    //finally
                    //{
                    //    rwLock.ExitWriteLock();
                    //}
                }

                return reference;
            //}
            //finally
            //{
            //    rwLock.ExitUpgradeableReadLock();
            //}
        }

        public bool IsReferenced(object context, object value)
        {
            // return this.reverseReferences.ContainsKey(value);
            GetReference(context, value);
            return true;
        }
    }

    public class ReferenceEqualComparer : IEqualityComparer<object>
    {
        public bool Equals(object x, object y)
        {
            return object.ReferenceEquals(x, y);
            //// TODO: introduce better abstraction
            //var a = x as LDAFeatureVector;
            //var b = y as LDAFeatureVector;

            //if (a == null || b == null)
            //    return x.Equals(y);

            //return a.Encoded == b.Encoded;
        }

        public int GetHashCode(object obj)
        {
            return obj.GetHashCode();
            //var a = obj as LDAFeatureVector;
            //return a == null ? obj.GetHashCode() : a.Encoded.GetHashCode();
        }
    }

}
