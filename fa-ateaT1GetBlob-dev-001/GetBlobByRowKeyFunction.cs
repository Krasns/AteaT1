using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;

namespace fa_ateaT1GetBlob_dev_001
{
    public static class GetBlobByRowKeyFunction
    {
        private static readonly string connectionString = "UseDevelopmentStorage=true";
        private static readonly string containerName = "weatherdata";

        [FunctionName("GetBlobByRowKeyFunction")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", Route = "Blob")] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("GetBlobByRowKeyFunction - C# HTTP trigger function processed a request.");

            string rowKey = req.Query["rowKey"];
            if (string.IsNullOrEmpty(rowKey))
            {
                return new BadRequestObjectResult("Please provide a valid 'rowKey' query parameter.");
            }

            var blobServiceClient = new BlobServiceClient(connectionString);
            var blobContainerClient = blobServiceClient.GetBlobContainerClient(containerName);

            await foreach (BlobItem blobItem in blobContainerClient.GetBlobsAsync())
            {
                if (blobItem.Metadata.TryGetValue("RowKey", out string metadataRowKey) && metadataRowKey == rowKey)
                {
                    BlobClient blobClient = blobContainerClient.GetBlobClient(blobItem.Name);
                    BlobDownloadInfo download = await blobClient.DownloadAsync();

                    using (var reader = new System.IO.StreamReader(download.Content))
                    {
                        string content = await reader.ReadToEndAsync();
                        return new OkObjectResult(content);
                    }
                }
            }

            return new NotFoundObjectResult("Blob with the specified RowKey not found.");
        }
    }
}
