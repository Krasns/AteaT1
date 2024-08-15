using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Azure.Data.Tables;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using fa_ateaT1TimeTrigger_dev_001.Model;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;

namespace fa_ateaT1TimeTrigger_dev_001
{
    public class Function1
    {
        private static readonly HttpClient client = new HttpClient();
        private static readonly string connectionString = "UseDevelopmentStorage=true";
        private static readonly string containerName = "weatherdata";
        private static readonly string tableName = "ApiCallResults";

        [FunctionName("Function1")]
        public static async Task Run([TimerTrigger("0 * * * * *")] TimerInfo myTimer, ILogger log)
        {
            log.LogInformation($"Function1 - C# Timer trigger function executed at: {DateTime.Now}");

            string apiUrl = "https://api.openweathermap.org/data/2.5/weather?q=London&appid=1bfedde2b12b74b0ea1c1ed6ea4b2048";
            HttpResponseMessage apiResult = await client.GetAsync(apiUrl);
            string status = apiResult.IsSuccessStatusCode ? "Success" : "Failure";
            log.LogInformation($"API call status: {status}");

            TableServiceClient serviceClient = new TableServiceClient(connectionString);
            TableClient tableClient = serviceClient.GetTableClient(tableName);
            await tableClient.CreateIfNotExistsAsync();
            string _RowKey = Guid.NewGuid().ToString();

            var entity = new ApiCallResultEntity
            {
                PartitionKey = "ApiCall",
                RowKey = _RowKey,
                CallTime = DateTime.UtcNow,
                Status = status
            };

            await tableClient.AddEntityAsync(entity);
            log.LogInformation("API call result stored in table storage.");

            if (apiResult.IsSuccessStatusCode)
            {
                string apiJSON = await apiResult.Content.ReadAsStringAsync();

                BlobServiceClient blobServiceClient = new BlobServiceClient(connectionString);
                BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(containerName);
                await containerClient.CreateIfNotExistsAsync();

                string blobName = $"{containerName}-{DateTime.Now:yyyyMMddHHmmss}.json";
                BlobClient blobClient = containerClient.GetBlobClient(blobName);
                Dictionary<string,string> metaData = new Dictionary<string,string>{ {"RowKey", _RowKey } };

                using (var stream = new System.IO.MemoryStream(System.Text.Encoding.UTF8.GetBytes(apiJSON)))
                {
                    await blobClient.UploadAsync(stream, new BlobHttpHeaders { ContentType = "application/json" } , metaData);
                }

                log.LogInformation($"Weather data stored in blob: {blobName}");
            }
        }
    }
}
