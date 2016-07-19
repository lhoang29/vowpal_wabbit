namespace Microsoft.Content.Recommendations.TrainingRuntime.Reader
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    using WindowsAzure.Storage.Table;

    class TrackbackEntity: TableEntity
    {
        public TrackbackEntity()
        {
        }

        public long ReaderTimestamp { get; set; }

        public Guid BlobId { get; set; }

        public DateTime BlobDateTime { get; set; }

        public int Index { get; set; }

        public long EndingOffset { get; set; }

        public bool IsInit { get; set; }
    }
}
