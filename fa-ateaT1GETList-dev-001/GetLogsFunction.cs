using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Azure.Data.Tables;
using System.Linq;
using fa_ateaT1TimeTrigger_dev_001.Model;
using System.Collections.Generic;

namespace fa_ateaT1GETList_dev_001
{
    public static class GetLogsFunction
    {
        private static readonly string connectionString = "UseDevelopmentStorage=true";
        private static readonly string tableName = "ApiCallResults";

        [FunctionName("GetLogsFunction")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", Route = "logs")] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("GetLogsFunction - C# Get List function processed a request.");

            string from = req.Query["from"];
            string to = req.Query["to"];

            if (!DateTime.TryParse(from, out DateTime fromDate) || !DateTime.TryParse(to, out DateTime toDate))
            {
                log.LogInformation("GetLogsFunction - Error bad Request.");
                return new BadRequestObjectResult($"Please provide valid 'from' and 'to' date parameters - {DateTime.UtcNow.ToString()}. From - {from} , To -  {to}");
            }

            var serviceClient = new TableServiceClient(connectionString);
            var tableClient = serviceClient.GetTableClient(tableName);

            var query = tableClient.Query<ApiCallResultEntity>(entity =>
            entity.CallTime >= fromDate && entity.CallTime <= toDate);

            List<ApiCallResultEntity> results = query.ToList();

            return new OkObjectResult(results);
        }
    }
}
