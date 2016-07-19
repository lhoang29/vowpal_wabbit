using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.Azure;
using System.Text.RegularExpressions;
using System.Globalization;
using System.IO;
using System.Threading.Tasks.Dataflow;
using System.Threading;
using Microsoft.WindowsAzure.Storage.RetryPolicies;

namespace msn
{
    public class AzureDownloader
    {
        public static void Download(string outputDirectory, string prefix, DateTime startTimeInclusive, DateTime endTimeExclusive)
        {
            var storageAccount = CloudStorageAccount.Parse("DefaultEndpointsProtocol=http;AccountName=mwtstorage;AccountKey=q7ClzsHSNHYvkiaqKurcIZJBnqgbNXit6p5w+jXma8D+ylT4s+zseEfI5Ik8Lw3/L+n9HN4j916wqEzM1tstPA==");
            var blobClient = storageAccount.CreateCloudBlobClient();

            var blobsToDownload = new List<CloudBlockBlob>();

            Console.WriteLine("Downloading {0:yyyy-MM-dd} to {1:yyyy-MM-dd}", startTimeInclusive, endTimeExclusive);
            foreach (var container in blobClient.ListContainers(prefix))
	        {
                // find relevant folder
                DateTime containerDateTime;
                var match = Regex.Match(container.Name, "^" + Regex.Escape(prefix) + @"(?:(?:-archive-)|(?:complete))(\d{8})$");
		        if (!match.Success &
                    !DateTime.TryParseExact(match.Groups[1].Value, "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None, out containerDateTime))
                {
                    continue;
                }

                if (!(containerDateTime >= startTimeInclusive && containerDateTime < endTimeExclusive))
                {
                    continue;
                }

                Console.WriteLine("Checking "  + container.Name);

                // create directory
                var outputDir = Path.Combine(outputDirectory, container.Name);
                if (!Directory.Exists(outputDir))
                    Directory.CreateDirectory(outputDir);

                foreach (var listItem in container.ListBlobs())
                {
                    var blob = listItem as CloudBlockBlob;
                    if (blob == null)
                    {
                        continue;
                    }

                    var outputFile = Path.Combine(outputDir, blob.Name);
                    var fileInfo = new FileInfo(outputFile);

                    if (fileInfo.Exists && fileInfo.Length == blob.Properties.Length)
                    {
                        continue;
                    }

                    blobsToDownload.Add(blob);
                }
            }

            var totalSize = blobsToDownload.Sum(b => b.Properties.Length / 1024f / 1024f);
            //Console.WriteLine("Downloading {0} blobs of size {1:0.0}MB",
            //    blobsToDownload.Count, totalSize);

            long downloadedBytes = 0;
            var downloadQueue = new ActionBlock<CloudBlockBlob>(blob =>
                {
                    var outputFile = Path.Combine(outputDirectory, blob.Container.Name, blob.Name);
                    var options = new BlobRequestOptions
                    {
                        MaximumExecutionTime = new TimeSpan(0, 60, 0),
                        ServerTimeout = new TimeSpan(0, 60, 0),
                        RetryPolicy = new ExponentialRetry()
                    };

                    try
                    {
                        blob.DownloadToFile(outputFile, FileMode.Create, options: options);

                        Interlocked.Add(ref downloadedBytes, blob.Properties.Length);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Failed to download {0}: {1}",
                            outputFile, e.Message);
                    }
                },
                new ExecutionDataflowBlockOptions
                {
                    MaxDegreeOfParallelism = 8
                });

            foreach (var blob in blobsToDownload)
            {
                downloadQueue.Post(blob);
            }

            downloadQueue.Complete();
            while (!(downloadQueue.Completion.IsCompleted || downloadQueue.Completion.IsCanceled || downloadQueue.Completion.IsFaulted))
            {
                Console.Write("Progress {0:0.0}/{1:0.0}MB...\r",
                    downloadedBytes / 1024f / 1024f, totalSize);

                Thread.Sleep(500);
            }

            Console.WriteLine("\nDone");
        }
    }
}
