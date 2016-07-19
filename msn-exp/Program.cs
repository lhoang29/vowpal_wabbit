using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Auth;
using Microsoft.Azure.Batch.Common;
using Microsoft.Azure.Batch.FileStaging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using msn_common;
using System.Globalization;
using Newtonsoft.Json;
using System.Diagnostics;

namespace msn_exp
{
    class Program
    {
        static void Main(string[] args)
        {
            Job.JobMain(args);
        }
    }

    public static class Job
    {
        // files that are required on the compute nodes that run the tasks
        private const string MainExeName = "msn.exe"; // main executable

#if DEBUG
        private const string BuildFlavor = "Debug";
#else
        private const string BuildFlavor = "Release";
#endif

        private static void SaveJsonSweepArguments(string outFile, out int numSweeps)
        {
            var arguments = Util.Expand(
                    //new[] { "--id 1", "--id 2" },
                    new[] { "--cb_type ips", "--cb_type mtr", "--cb_type dr" },
                    new[] { 0.005, 0.01, 0.02, 0.05, 0.1 }.Select(l => string.Format(CultureInfo.InvariantCulture, "-l {0}", l))
                    //new[] { 0.005, 0.05 }.Select(l => string.Format(CultureInfo.InvariantCulture, "-l {0}", l))
                )
                .Select(a => "--quiet --cb_adf --rank_all --interact ud " + a)
                .ToList();

            var newEpsilons = new[] { .33333f, .25f, .2f, .15f, .1f, .05f };

            List<SweepArguments> settings =
            (
                from arg in arguments
                select new SweepArguments
                {
                    Arguments = arg,
                }
            ).ToList();

            File.WriteAllText(outFile, JsonConvert.SerializeObject(settings));

            numSweeps = settings.Count;
        }

        private static void SaveBlobList(string outFile, CloudStorageAccount cloudStorageAccount)
        {
            CloudBlobClient client = cloudStorageAccount.CreateCloudBlobClient();
            CloudBlobContainer container = client.GetContainerReference(SweepMetadata.DataRootContainer);

            List<string> trainBlobNames = container
                .GetDirectoryReference(SweepMetadata.TrainPath)
                .ListBlobs()
                .Select(b => ((CloudBlockBlob)b).Name)
                .ToList();
            File.WriteAllLines(outFile, trainBlobNames);

            List<string> testBlobNames = container
                .GetDirectoryReference(SweepMetadata.TestPath)
                .ListBlobs()
                .Select(b => ((CloudBlockBlob)b).Name)
                .ToList();
            File.AppendAllLines(outFile, testBlobNames);
        }

        public static void JobMain(string[] args)
        {
            // 1. Save list of sweep arguments to JSON file.
            // 2. Save list of blob names for train and test to text file.
            // 3. Upload both files and executables to Azure batch.
            // 4. Each task runs on one node, selects a subset of sweeps to work on and writes header & models to local storage first.
            // 5. Header and models are uploaded to Azure storage at the end.

            var watch = new Stopwatch();
            watch.Start();

            string exeDir = "azure"; // directory containing the msn.exe and its dependencies in order to run sweep on azure

            //Load the configuration
            Settings msnExpConfiguration = Settings.Default;
            AccountSettings accountSettings = AccountSettings.Default;

            CloudStorageAccount cloudStorageAccount = new CloudStorageAccount(
                new StorageCredentials(
                    accountSettings.StorageAccountName,
                    accountSettings.StorageAccountKey),
                accountSettings.StorageServiceUrl,
                useHttps: true);

            StagingStorageAccount stagingStorageAccount = new StagingStorageAccount(
                accountSettings.StorageAccountName,
                accountSettings.StorageAccountKey,
                cloudStorageAccount.BlobEndpoint.ToString());

            using (BatchClient client = BatchClient.Open(new BatchSharedKeyCredentials(
                accountSettings.BatchServiceUrl,
                accountSettings.BatchAccountName,
                accountSettings.BatchAccountKey)))
            {
                string stagingContainer = null;
                int numSweeps = 0;
                Job.SaveJsonSweepArguments(SweepMetadata.SweepJsonFile, out numSweeps);
                Job.SaveBlobList(SweepMetadata.BlobListFile, cloudStorageAccount);

                int numCores = SweepMetadata.VMSizeToProcCount[msnExpConfiguration.PoolVMSize];
                int numThreadsPerNode = 1 * numCores;
                int nodeCount = (int)Math.Ceiling((float)numSweeps / numThreadsPerNode);
                if (nodeCount * numCores > SweepMetadata.AzureBatchMaxCoreCount)
                {
                    nodeCount = SweepMetadata.AzureBatchMaxCoreCount / numCores;
                    numThreadsPerNode = Math.Max(numThreadsPerNode, (int)Math.Ceiling((float)numSweeps / nodeCount));
                }

                //OSFamily 4 == OS 2012 R2. You can learn more about os families and versions at:
                //http://msdn.microsoft.com/en-us/library/azure/ee924680.aspx
                CloudPool pool = client.PoolOperations.CreatePool(
                    msnExpConfiguration.PoolId,
                    targetDedicated: nodeCount,
                    osFamily: "4",
                    virtualMachineSize: msnExpConfiguration.PoolVMSize);
                pool.MaxTasksPerComputeNode = 1; // each task consists of several sweeps, so only one task per node

                Console.WriteLine("Adding pool {0} with {1} nodes", msnExpConfiguration.PoolId, nodeCount);

                try
                {
                    CreatePoolIfNotExistAsync(client, pool).Wait();
                }
                catch (AggregateException ae)
                {
                    // Go through all exceptions and dump useful information
                    ae.Handle(x =>
                    {
                        Console.Error.WriteLine("Creating pool ID {0} failed", msnExpConfiguration.PoolId);
                        if (x is BatchException)
                        {
                            BatchException be = x as BatchException;

                            Console.WriteLine(be.ToString());
                            Console.WriteLine();
                        }
                        else
                        {
                            Console.WriteLine(x);
                        }

                        // can't continue without a pool
                        return false;
                    });
                }

                try
                {
                    Console.WriteLine("Creating job: " + msnExpConfiguration.JobId);

                    Job.CreateNewJob(client, msnExpConfiguration.PoolId, msnExpConfiguration.JobId).Wait();

                    // create file staging objects that represent the executable and its dependent assembly to run as the task.
                    // These files are copied to every node before the corresponding task is scheduled to run on that node.
                    List<IFileStagingProvider> filesToStage = Directory
                        .EnumerateFiles(exeDir) // requires .NET 4.5
                        .Where(file => file.ToLower().EndsWith("exe") || file.ToLower().EndsWith("dll"))
                        .Select(path => (IFileStagingProvider)(new FileToStage(path, stagingStorageAccount)))
                        .ToList();
                    filesToStage.Add(new FileToStage(SweepMetadata.SweepJsonFile, stagingStorageAccount));
                    filesToStage.Add(new FileToStage(SweepMetadata.BlobListFile, stagingStorageAccount));

                    // initialize a collection to hold the tasks that will be submitted in their entirety
                    var tasksToRun = new List<CloudTask>(numThreadsPerNode);

                    for (int i = 0; i < nodeCount; i++)
                    {
                        var task = new CloudTask(
                            id: "msn_sweep_" + i,
                            commandline: String.Format
                            (
                                "{0} --AzureSweep {1} {2} {3} {4}",
                                MainExeName,
                                i,
                                numThreadsPerNode,
                                accountSettings.StorageAccountName,
                                accountSettings.StorageAccountKey
                            )
                        );

                        //This is the list of files to stage to a container -- for each job, one container is created and 
                        //files all resolve to Azure Blobs by their name (so two tasks with the same named file will create just 1 blob in
                        //the container).
                        task.FilesToStage = filesToStage;

                        tasksToRun.Add(task);
                    }

                    // Commit all the tasks to the Batch Service. Ask AddTask to return information about the files that were staged.
                    // The container information is used later on to remove these files from Storage.
                    var fsArtifactBag = new ConcurrentBag<ConcurrentDictionary<Type, IFileStagingArtifact>>();
                    client.JobOperations.AddTask(msnExpConfiguration.JobId, tasksToRun, fileStagingArtifacts: fsArtifactBag);

                    // loop through the bag of artifacts, looking for the one that matches our staged files. Once there,
                    // capture the name of the container holding the files so they can be deleted later on if that option
                    // was configured in the settings.
                    foreach (var fsBagItem in fsArtifactBag)
                    {
                        IFileStagingArtifact fsValue;
                        if (fsBagItem.TryGetValue(typeof(FileToStage), out fsValue))
                        {
                            SequentialFileStagingArtifact stagingArtifact = fsValue as SequentialFileStagingArtifact;
                            if (stagingArtifact != null)
                            {
                                stagingContainer = stagingArtifact.BlobContainerCreated;
                                Console.WriteLine(
                                    "Uploaded files to container: {0}.",
                                    stagingArtifact.BlobContainerCreated);
                            }
                        }
                    }

                    //Get the job to monitor status.
                    CloudJob job = client.JobOperations.GetJob(msnExpConfiguration.JobId);

                    Console.Write("Waiting for tasks to complete ...   ");
                    // Wait 20 minutes for all tasks to reach the completed state. The long timeout is necessary for the first
                    // time a pool is created in order to allow nodes to be added to the pool and initialized to run tasks.
                    IPagedEnumerable<CloudTask> ourTasks = job.ListTasks(new ODATADetailLevel(selectClause: "id"));
                    client.Utilities.CreateTaskStateMonitor().WaitAll(ourTasks, TaskState.Completed, TimeSpan.FromHours(24));
                    Console.WriteLine("tasks are done.");

                    foreach (CloudTask t in ourTasks)
                    {
                        Console.WriteLine("Task " + t.Id);
                        Console.WriteLine("stdout:" + Environment.NewLine + t.GetNodeFile(Microsoft.Azure.Batch.Constants.StandardOutFileName).ReadAsString());
                        Console.WriteLine();
                        Console.WriteLine("stderr:" + Environment.NewLine + t.GetNodeFile(Microsoft.Azure.Batch.Constants.StandardErrorFileName).ReadAsString());
                    }
                }
                finally
                {
                    //Delete the pool that we created
                    if (msnExpConfiguration.ShouldDeletePool)
                    {
                        Console.WriteLine("Deleting pool: {0}", msnExpConfiguration.PoolId);
                        client.PoolOperations.DeletePool(msnExpConfiguration.PoolId);
                    }

                    //Delete the job that we created
                    if (msnExpConfiguration.ShouldDeleteJob)
                    {
                        Console.WriteLine("Deleting job: {0}", msnExpConfiguration.JobId);
                        client.JobOperations.DeleteJob(msnExpConfiguration.JobId);
                    }

                    //Delete the containers we created
                    if (msnExpConfiguration.ShouldDeleteContainer)
                    {
                        DeleteContainers(accountSettings, stagingContainer);
                    }
                    Console.WriteLine("Total time elapsed: {0}ms, estimated cost for entire pool of {1} nodes: ${2}",
                        watch.ElapsedMilliseconds, nodeCount, 
                        SweepMetadata.VMSizeToCostPerHour[msnExpConfiguration.PoolVMSize] * nodeCount * watch.ElapsedMilliseconds / (1000 * 60 * 60));
                }
            }
        }

        /// <summary>
        /// create a client for accessing blob storage
        /// </summary>
        private static CloudBlobClient GetCloudBlobClient(string accountName, string accountKey, string accountUrl)
        {
            StorageCredentials cred = new StorageCredentials(accountName, accountKey);
            CloudStorageAccount storageAccount = new CloudStorageAccount(cred, accountUrl, useHttps: true);
            CloudBlobClient client = storageAccount.CreateCloudBlobClient();

            return client;
        }

        /// <summary>
        /// Delete the containers in Azure Storage which are created by this sample.
        /// </summary>
        private static void DeleteContainers(AccountSettings accountSettings, string fileStagingContainer)
        {
            CloudBlobClient client = GetCloudBlobClient(
                accountSettings.StorageAccountName,
                accountSettings.StorageAccountKey,
                accountSettings.StorageServiceUrl);

            //Delete the file staging container
            if (!string.IsNullOrEmpty(fileStagingContainer))
            {
                CloudBlobContainer container = client.GetContainerReference(fileStagingContainer);
                Console.WriteLine("Deleting container: {0}", fileStagingContainer);
                container.DeleteIfExists();
            }
        }

        /// <summary>
        /// Creates a pool if it doesn't already exist.  If the pool already exists, this method resizes it to meet the expected
        /// targets specified in settings.
        /// </summary>
        /// <param name="batchClient">The BatchClient to create the pool with.</param>
        /// <param name="pool">The pool to create.</param>
        /// <returns>An asynchronous <see cref="Task"/> representing the operation.</returns>
        private static async Task CreatePoolIfNotExistAsync(BatchClient batchClient, CloudPool pool)
        {
            bool successfullyCreatedPool = false;

            int poolTargetNodeCount = pool.TargetDedicated ?? 0;
            string poolNodeVirtualMachineSize = pool.VirtualMachineSize;
            string poolId = pool.Id;

            // Attempt to create the pool
            try
            {
                // Create an in-memory representation of the Batch pool which we would like to create.  We are free to modify/update 
                // this pool object in memory until we commit it to the service via the CommitAsync method.
                Console.WriteLine("Attempting to create pool: {0}", pool.Id);

                // Create the pool on the Batch Service
                await pool.CommitAsync().ConfigureAwait(continueOnCapturedContext: false);

                successfullyCreatedPool = true;
                Console.WriteLine("Created pool {0} with {1} {2} nodes",
                    poolId,
                    poolTargetNodeCount,
                    poolNodeVirtualMachineSize);
            }
            catch (BatchException e)
            {
                // Swallow the specific error code PoolExists since that is expected if the pool already exists
                if (e.RequestInformation != null &&
                    e.RequestInformation.AzureError != null &&
                    e.RequestInformation.AzureError.Code == BatchErrorCodeStrings.PoolExists)
                {
                    // The pool already existed when we tried to create it
                    successfullyCreatedPool = false;
                    Console.WriteLine("The pool already existed when we tried to create it");
                }
                else
                {
                    throw; // Any other exception is unexpected
                }
            }

            // If the pool already existed, make sure that its targets are correct
            if (!successfullyCreatedPool)
            {
                CloudPool existingPool = await batchClient.PoolOperations.GetPoolAsync(poolId).ConfigureAwait(continueOnCapturedContext: false);

                // If the pool doesn't have the right number of nodes and it isn't resizing then we need
                // to ask it to resize
                if (existingPool.CurrentDedicated != poolTargetNodeCount &&
                    existingPool.AllocationState != AllocationState.Resizing)
                {
                    // Resize the pool to the desired target.  Note that provisioning the nodes in the pool may take some time
                    await existingPool.ResizeAsync(poolTargetNodeCount).ConfigureAwait(continueOnCapturedContext: false);
                }
            }
        }

        private static async Task CreateNewJob(BatchClient client, string poolId, string jobId)
        {
            try
            {
                client.JobOperations.DeleteJob(jobId);
            }
            catch { /* ignore exception if failed to delete */ }

            CloudJob unboundJob = client.JobOperations.CreateJob();
            unboundJob.Id = jobId;
            unboundJob.PoolInformation = new PoolInformation() { PoolId = poolId };
            await unboundJob.CommitAsync(
                new BatchClientBehavior[]
                { 
                    RetryPolicyProvider.ExponentialRetryProvider
                    (
                        deltaBackoff: TimeSpan.FromMilliseconds(500),
                        maxRetries: 5
                    )
                }
            );
        }

        public static void DownloadSweepOutput(string outPath, IList<string> sweepOutputContainerNames)
        {
            Directory.CreateDirectory(outPath);

            AccountSettings accountSettings = AccountSettings.Default;
            
            var storageAccount = new CloudStorageAccount(
                new StorageCredentials(
                    accountSettings.StorageAccountName,
                    accountSettings.StorageAccountKey),
                accountSettings.StorageServiceUrl,
                useHttps: true);

            CloudBlobClient client = storageAccount.CreateCloudBlobClient();
            CloudBlobContainer container = client.GetContainerReference(SweepMetadata.DataRootContainer);

            Parallel.For(0, sweepOutputContainerNames.Count, new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount }, i =>
            {
                container
                    .GetDirectoryReference(sweepOutputContainerNames[i])
                    .GetBlockBlobReference(SweepMetadata.OutputCsvFile)
                    .DownloadToFile(Path.Combine(outPath, sweepOutputContainerNames[i] + "-" + SweepMetadata.OutputCsvFile), FileMode.CreateNew);
            });
        }

        public static void CombineSweepOutput(string outFile, string inPath)
        {
            string[] csvFiles = Directory.GetFiles(inPath, "*.csv");
            var lines = new List<string>();
            var header = string.Empty;
            foreach (string f in csvFiles)
            {
                string[] content = File.ReadAllLines(f);
                header = content[0];
                lines.AddRange(content.Skip(1));
            }
            File.WriteAllLines(outFile, new string[] { header });
            File.AppendAllLines(outFile, lines);
        }
    }
}
