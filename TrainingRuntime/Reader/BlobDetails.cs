namespace Microsoft.Content.Recommendations.TrainingRuntime.Reader
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Text.RegularExpressions;
    using System.Threading.Tasks;

    using Microsoft.WindowsAzure.Storage.Blob;

    internal class BlobDetails
    {
        internal const int ReferenceIndex = 999;

        internal const string RefBlobGuidFormat = "1000{0:D4}-{1:D2}{2:D2}-77{3:D2}-0000-111111111111";

        internal BlobDetails(CloudBlockBlob blob, DateTime containerDateTime, int blobHour, int blobIndex, Guid blobGuid)
        {
            this.Blob = blob;          
            
            this.Index = blobIndex;
            
            this.BlobDateTime = containerDateTime.AddHours(blobHour);

            if (blobIndex == ReferenceIndex)
            {
                this.BlobGuid = Guid.Parse(string.Format(RefBlobGuidFormat, this.BlobDateTime.Year, this.BlobDateTime.Month, this.BlobDateTime.Day, blobHour));
            }
            else
            {
                this.BlobGuid = blobGuid;
            }

            this.Length = blob.Properties.Length;
            this.ResumeOffset = 0;
        }

        internal BlobDetails(string filename, DateTime containerDateTime, int blobHour, int blobIndex, Guid blobGuid)
        {
            this.Filename = filename;

            this.Index = blobIndex;

            this.BlobDateTime = containerDateTime.AddHours(blobHour);

            if (blobIndex == ReferenceIndex)
            {
                this.BlobGuid = Guid.Parse(string.Format(RefBlobGuidFormat, this.BlobDateTime.Year, this.BlobDateTime.Month, this.BlobDateTime.Day, blobHour));
            }
            else
            {
                this.BlobGuid = blobGuid;
            }

            this.Length = 0;
            this.ResumeOffset = 0;
        }

        internal Guid BlobGuid { get; set; }

        internal DateTime BlobDateTime { get; private set; }

        internal CloudBlockBlob Blob { get; private set; }

        internal string Filename { get; private set; }

        internal int Index { get; private set; }

        internal long Length { get; set; }

        internal long ResumeOffset { get; set; }

        public override bool Equals(object obj)
        {
            var blobDetails = obj as BlobDetails;
            if (blobDetails == null)
            {
                return false;
            }

            return this.BlobGuid.Equals(blobDetails.BlobGuid);
        }

        public override int GetHashCode()
        {
            return this.BlobGuid.GetHashCode();
        }
    }

    internal class BlobDetailsComparer : IComparer<BlobDetails>
    {
        public int Compare(BlobDetails x, BlobDetails y)
        {
            if (x == null && y == null)
            {
                return 0;
            }
            else if (x == null)
            {
                return -1;
            }
            else if (y == null)
            {
                return 1;
            }

            var res = x.BlobDateTime.CompareTo(y.BlobDateTime);
            if (res == 0)
            {
                if ((x.Index ^ y.Index) != 0)
                {
                    if (x.Index == BlobDetails.ReferenceIndex)
                    {
                        return -1;
                    }
                    else if (y.Index == BlobDetails.ReferenceIndex)
                    {
                        return 1;
                    }
                }

                res = x.Index.CompareTo(y.Index);
            }
            
            return res;
        }
    }
}
