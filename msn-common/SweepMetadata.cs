using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace msn_common
{
    public static class SweepMetadata
    {
        public static readonly string Command = "--AzureSweep";

        public static readonly string DataRootContainer = "msn-exp";
        public static readonly string TrainPath = "train";
        public static readonly string TestPath = "test";
        public static readonly string DataTrainContainer = DataRootContainer + "/" + TrainPath;
        public static readonly string DataTestContainer = DataRootContainer + "/" + TestPath;

        public static readonly string CookedDictionaryFile = "dict.json.gz";
        public static readonly string SweepJsonFile = "sweep.json"; // list of sweep parameters
        public static readonly string BlobListFile = "blob_list.txt"; // list of train & test blob names
        
        public static readonly string OutputCsvFile = "detail.csv";

        // Default Limit of Azure Batch: https://azure.microsoft.com/en-us/documentation/articles/batch-quota-limit/
        public static readonly int AzureBatchMaxCoreCount = 400;

        public static readonly string LocalDataDir = "msn-data";

        public static readonly Dictionary<string, int> VMSizeToProcCount =
            new Dictionary<string, int>() 
            { 
                {"extrasmall", 1},
                {"small", 1},
                {"medium", 2},
                {"large", 4},
                {"extralarge", 8}
            };

        public static readonly Dictionary<string, float> VMSizeToCostPerHour =
            new Dictionary<string, float>() 
            { 
                {"extrasmall", 0.02f},
                {"small", 0.08f},
                {"medium", 0.16f},
                {"large", 0.32f},
                {"extralarge", 0.64f}
            };
    }
}
