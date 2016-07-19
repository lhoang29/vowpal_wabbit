namespace Microsoft.Content.Recommendations.TrainingRuntime
{
    using System;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading.Tasks;

    using Microsoft.WindowsAzure;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;

    public sealed class BlobLock: IDisposable
    {
        private bool disposed = false;

        public BlobLock(CloudBlockBlob blob, TimeSpan leaseTime)
        {
            if (blob == null)
            {
                return;
            }

            this.Blob = blob;

            this.BlobExists = this.Blob.Exists();

            this.LeaseId = this.TryAcquireLease(leaseTime);
        }

        public CloudBlockBlob Blob { get; private set; }

        public string LeaseId { get; private set; }

        public bool BlobExists { get; private set; }

        public bool HasLease
        {
            get
            {
                return !string.IsNullOrEmpty(this.LeaseId);
            }
        }

        ~BlobLock()
        {
            this.Dispose(false);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private string TryAcquireLease(TimeSpan leaseTime)
        {
            try
            {
                string proposedLeaseId = Guid.NewGuid().ToString();

                var leaseId = this.Blob.AcquireLease(leaseTime, proposedLeaseId);

                return leaseId;
            }
            catch (Exception ex)
            {
                Trace.TraceInformation(ex.Message);
                return null;
            }
        }

        private void ReleaseLease()
        {
            if (!this.HasLease || this.Blob == null)
            {
                return;
            }

            try
            {
                this.Blob.ReleaseLease(new AccessCondition() { LeaseId = this.LeaseId });
            }
            catch (Exception ex)
            {
                Trace.TraceInformation("Failed to release lease on blob {0}: {1}", this.Blob.Name, ex.Message);
            }
        }

        private void Dispose(bool disposing)
        {
            if (disposed)
            {
                return;
            }

            this.ReleaseLease();

            disposed = true;
        }
    }
}
