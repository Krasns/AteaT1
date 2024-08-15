using Azure;
using Azure.Data.Tables;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace fa_ateaT1TimeTrigger_dev_001.Model
{
    public class ApiCallResultEntity : ITableEntity
    {
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public DateTime CallTime { get; set; }
        public string Status { get; set; }

        public ETag ETag { get; set; }
        public DateTimeOffset? Timestamp { get; set; }
    }
}
