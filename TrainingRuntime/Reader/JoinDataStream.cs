namespace Microsoft.Content.Recommendations.TrainingRuntime.Reader
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;

    public class JoinDataStream: Stream
    {
        private const int ConflictRetry = 5;

        private static readonly byte[] BlobEndChars = {0x5d, 0x7d}; // Last block of join data blob: ']', '}'

        private static readonly string BlobHeaderFormat = "{{\"blob\":\"{0}\",\"data\":[";

        private bool disposed = false;

        private MemoryStream joinDataHeaderStream;

        private CloudBlockBlob blockBlob;

        private Stream blobStream;

        private long currentOffset;

        private long targetLength;

        public JoinDataStream(CloudBlockBlob blob, long? targetLength = null, int index = -1, int hour = -1)
        {
            this.InitializeStream(blob, Guid.Empty, 0, targetLength, index, hour);
        }

        public JoinDataStream(CloudBlockBlob blob, Guid blobGuid, long resumeOffset, long? targetLength = null, int index = -1, int hour = -1)
        {
            this.InitializeStream(blob, blobGuid, resumeOffset, targetLength, index, hour);
        }

        public int Index { get; private set; }

        public int Hour { get; private set; }

        public bool IsReference
        {
            get
            {
                return this.Index == BlobDetails.ReferenceIndex;
            }
        }

        public string Name
        {
            get
            {
                return (this.blockBlob != null) ? this.blockBlob.Name : string.Empty;
            }
        }

        public long ResumeOffset
        {
            get
            {
                return this.targetLength;
            }
        }
        
        public override bool CanRead
        {
            get
            {
                return this.blobStream != null;
            }
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (this.blobStream == null)
            {
                return 0;
            }

            if (this.joinDataHeaderStream != null)
            {
                int bytesRead = this.joinDataHeaderStream.Read(buffer, offset, count);
                if (bytesRead != 0)
                {
                    return bytesRead;
                }

                this.joinDataHeaderStream.Dispose();
                this.joinDataHeaderStream = null;
            }

            for (var i = 0; i < ConflictRetry; i++)
            {
                try
                {
                    // never go over target length
                    int readCount = (this.currentOffset + count) > this.targetLength ? Convert.ToInt32(this.targetLength - this.currentOffset) : count;
                    if (readCount == 0)
                    {
                        return 0;
                    }

                    int bytesRead = this.blobStream.Read(buffer, offset, readCount);
                    if (bytesRead < 0)
                    {
                        Trace.TraceError("Blob stream read returned less than 0: " + this.Name);
                        return bytesRead;
                    }

                    this.currentOffset += bytesRead;

                    if (bytesRead != 0)
                    {
                        if (this.targetLength == this.currentOffset)
                        {
                            if (bytesRead < 2)
                            {
                                Trace.TraceWarning(string.Format("Blob stream read returned {0} at offset {1} end char {2}: {3}", bytesRead, this.currentOffset, buffer[0], this.Name));
                                buffer[bytesRead - 1] = BlobEndChars[1];
                            }
                            else
                            {
                                buffer[bytesRead - 2] = BlobEndChars[0];
                                buffer[bytesRead - 1] = BlobEndChars[1];
                            }
                        }
                        else if (this.targetLength - 1 == this.currentOffset)
                        {
                            Trace.TraceWarning(string.Format("Blob stream read at target length - 1 at offset {0} end char {1}: {2}", this.currentOffset, buffer[bytesRead - 1], this.Name));
                            buffer[bytesRead - 1] = BlobEndChars[0];
                        }
                    }

                    return bytesRead;
                }
                catch (StorageException)
                {
                    // dispose current stream
                    this.blobStream.Dispose();
                }
                catch (Exception ex)
                {
                    // other error exceptions
                    throw ex;
                }

                // TODO
                this.blobStream = this.blockBlob.OpenRead();
                this.blobStream.Seek(this.currentOffset, SeekOrigin.Begin);
            }

            // can't recover from StorageException, skip data (may have other exception from parse due to partial data)
            this.currentOffset = this.targetLength;

            return 0;
        }

        public override bool CanSeek
        {
            get
            {
                return false;
            }
        }

        public override bool CanWrite
        {
            get
            {
                return false;
            }
        }

        public override void Flush()
        {
            throw new NotImplementedException();
        }

        public override long Length
        {
            get
            {
                return (this.blobStream != null) ? this.blobStream.Length : 0;
            }
        }

        public override long Position
        {
            get
            {
                return (this.blobStream != null) ? this.blobStream.Position : 0;
            }

            set
            {
                throw new NotImplementedException();
            }
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotImplementedException();
        }

        public override void SetLength(long value)
        {
            throw new NotImplementedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotImplementedException();
        }

        protected override void Dispose(bool disposing)
        {
            if (disposed)
            {
                return;
            }

            if (disposing && this.blobStream != null)
            {
                this.blobStream.Dispose();
            }

            disposed = true;

            base.Dispose(disposing);
        }

        private void InitializeStream(CloudBlockBlob blob, Guid blobGuid, long resumeOffset, long? targetLength, int index, int hour)
        {
            this.blockBlob = blob;
            this.blockBlob.StreamMinimumReadSizeInBytes = Constants.StreamMinimumReadSizeInBytes;
            this.Index = index;
            this.Hour = hour;

            this.blobStream = this.blockBlob.OpenRead();

            if (targetLength != null && targetLength.HasValue)
            {
                this.targetLength = targetLength.Value;
            }
            else
            {
                this.targetLength = this.blobStream.Length;
            }

            if (resumeOffset == 0)
            {
                this.currentOffset = 0;
            }
            else
            {
                this.ResumeJoinDataBlob(blobGuid, resumeOffset);
            }
        }

        private void ResumeJoinDataBlob(Guid blobGuid, long resumeOffset)
        {
            bool isValid = false;

            if (resumeOffset < 3 || resumeOffset > this.targetLength)
            {
                isValid = false;
            }
            else
            {
                for (var i = 0; i < ConflictRetry; i++)
                {
                    try
                    {
                        this.blobStream.Seek(resumeOffset - 3, SeekOrigin.Begin);
                        var firstByte = this.blobStream.ReadByte();
                        switch (firstByte)
                        {
                            case '[':
                            case ',':
                                // new data available
                                isValid = true;
                                break;

                            default:
                                // unexpected data, ignore resume offset
                                isValid = false;
                                break;
                        }

                        break;
                    }
                    catch (StorageException)
                    {
                        // dispose current stream
                        this.blobStream.Dispose();
                    }
                    catch (Exception ex)
                    {
                        // other error exceptions
                        throw ex;
                    }

                    this.blobStream = this.blockBlob.OpenRead();
                }
            }

            if (!isValid)
            {
                // do nothing and move to the end
                this.currentOffset = this.targetLength;
                return;
            }

            this.currentOffset = this.blobStream.Position;

            this.joinDataHeaderStream = new MemoryStream(Encoding.UTF8.GetBytes(string.Format(BlobHeaderFormat, blobGuid.ToString())));
        }
    }
}
