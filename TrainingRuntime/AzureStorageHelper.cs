namespace Microsoft.Content.Recommendations.TrainingRuntime
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Net;
    using System.Text;
    using System.Threading.Tasks;

    using Microsoft.WindowsAzure;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.Queue;
    using Microsoft.WindowsAzure.Storage.RetryPolicies;

    public static class AzureStorageHelper
    {
        public static CloudQueue GetQueue(string connectionString, string queueName, IRetryPolicy defaultRetryPolicy = null)
        {
            ServicePointManager.UseNagleAlgorithm = false;
            ServicePointManager.DefaultConnectionLimit = 48;

            try
            {
                CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);

                CloudQueueClient queueClient = storageAccount.CreateCloudQueueClient();
                if (defaultRetryPolicy != null)
                {
                    queueClient.DefaultRequestOptions.RetryPolicy = defaultRetryPolicy;
                }

                var queue = queueClient.GetQueueReference(queueName);

                queue.CreateIfNotExists();
                return queue;
            }
            catch (StorageException ex)
            {
                Trace.TraceError("Get storage queue failed: {0}", ex.Message);
            }

            return null;
        }

        public static CloudBlobContainer GetBlobContainer(string connectionString, string containerName, bool doCreate = false)
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);

            return GetBlobContainer(storageAccount, containerName, doCreate);
        }

        public static CloudBlobContainer GetBlobContainer(CloudStorageAccount account, string containerName, bool doCreate = false)
        {
            try
            {
                var blobClient = account.CreateCloudBlobClient();

                var container = blobClient.GetContainerReference(containerName);

                if (doCreate)
                {
                    container.CreateIfNotExists();
                }

                return container;
            }
            catch (StorageException ex)
            {
                Trace.TraceError("Failed to get container: {0}", ex.Message);
            }

            return null;
        }

        public static IEnumerable<IListBlobItem> ListBlobs(CloudBlobContainer container, string prefix = "")
        {
            BlobContinuationToken continuationToken = null;
            var blobRequestOptions = new BlobRequestOptions();
            var operationContext = new OperationContext();

            do
            {
                var result = container.ListBlobsSegmented(prefix, true, BlobListingDetails.None, null, continuationToken, blobRequestOptions, operationContext);

                foreach (var res in result.Results)
                {
                    yield return res;
                }

                continuationToken = result.ContinuationToken;
            }
            while (continuationToken != null);
        }

        public static IEnumerable<CloudBlobContainer> ListContainers(CloudBlobClient blobClient, string prefix = "")
        {
            BlobContinuationToken continuationToken = null;

            do
            {
                var result = blobClient.ListContainersSegmented(prefix, ContainerListingDetails.None, null, continuationToken);

                foreach (var res in result.Results)
                {
                    yield return res;
                }

                continuationToken = result.ContinuationToken;
            }
            while (continuationToken != null);
        }

        public static int GetQueueMessageCount(CloudQueue queue)
        {
            int messageCount = 0;

            try
            {
                queue.FetchAttributes();
                int? approxMessageCount = queue.ApproximateMessageCount;
                messageCount = approxMessageCount.HasValue ? approxMessageCount.Value : 0;
            }
            catch (Exception ex)
            {
                Trace.TraceError("Failed to get queue length: {0}", ex.Message);
            }

            return messageCount;
        }
    }
}
