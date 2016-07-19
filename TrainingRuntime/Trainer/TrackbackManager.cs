namespace Microsoft.Content.Recommendations.TrainingRuntime.Trainer
{
    using System;
    using System.IO;
    using System.Text;

    public class TrackbackManager
    {
        private long maxMemoryUsage;

        private MemoryStream trackbackDataStream;

        public TrackbackManager(long maxMemoryUsage)
        {
            this.maxMemoryUsage = maxMemoryUsage;
            this.trackbackDataStream = new MemoryStream();
        }

        public void Reset()
        {
            if (this.trackbackDataStream != null)
            {
                this.trackbackDataStream.Dispose();
            }

            this.trackbackDataStream = new MemoryStream();
        }

        public void Save(Stream targetStream)
        {
            if (this.trackbackDataStream == null || this.trackbackDataStream.Length == 0)
            {
                return;
            }

            this.trackbackDataStream.Flush();
            this.trackbackDataStream.Position = 0;

            this.trackbackDataStream.CopyTo(targetStream);
        }

        public void Log(string message)
        {
            if (this.trackbackDataStream == null || string.IsNullOrEmpty(message))
            {
                return;
            }

            if (this.trackbackDataStream.Length >= this.maxMemoryUsage)
            {
                return;
            }

            var bytes = Encoding.UTF8.GetBytes(message + Environment.NewLine);
            this.trackbackDataStream.Write(bytes, 0, bytes.Length);
        }
    }
}
