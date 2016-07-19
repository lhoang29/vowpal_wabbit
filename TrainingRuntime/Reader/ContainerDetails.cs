namespace Microsoft.Content.Recommendations.TrainingRuntime.Reader
{
    using System;

    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;

    internal class ContainerDetails
    {
        internal CloudBlobContainer Container { get; set; }

        internal DateTime DateTime { get; set; }
    }
}
