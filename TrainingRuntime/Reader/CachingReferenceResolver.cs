namespace Microsoft.Content.Recommendations.TrainingRuntime.Reader
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;

    using Newtonsoft.Json.Serialization;
using System.Threading;

    public class CachingReferenceResolver : IReferenceResolver
    {
        //private readonly ReaderWriterLockSlim rwLock = new ReaderWriterLockSlim();
        private readonly object objectLock = new object();
        private readonly Dictionary<string, object> references;
        private readonly Queue<string> evictionQueue;
        private readonly int maxCapacity;

        public CachingReferenceResolver(int maxCapacity)
        {
            this.maxCapacity = maxCapacity;
            this.references = new Dictionary<string, object>(maxCapacity);
            this.evictionQueue = new Queue<string>(maxCapacity);
        }

        public int Count
        {
            get
            {
                return this.references.Count;
            }
        }

        public void AddReference(object context, string reference, object value)
        {
            lock (this.objectLock)
            //try
            {
            //    this.rwLock.EnterWriteLock();

                //while (this.references.Count >= this.maxCapacity)
                //{
                //    this.references.Remove(this.evictionQueue.Dequeue());
                //}

                this.references[reference] = value;
                //this.evictionQueue.Enqueue(reference);
            }
            //finally
            //{
            //    this.rwLock.ExitWriteLock();
            //}
        }

        public object ResolveReference(object context, string reference)
        {
            //try
            //{
            //    this.rwLock.EnterReadLock();

                object item;
                if (this.references.TryGetValue(reference, out item))
                {
                    return item;
                }

                return null;
            //}
            //finally
            //{
            //    this.rwLock.ExitReadLock();
            //}
        }

        public string GetReference(object context, object value)
        {
            throw new NotImplementedException();
        }

        public bool IsReferenced(object context, object value)
        {
            throw new NotImplementedException();
        }
    }
}
