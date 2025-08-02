# Azure Job Container for CANedge MDF to Parquet Processing

This container implements automated backlog processing for CANedge MDF files in Azure Blob Storage.

## Container Description

This container runs the `process_backlog_azure.py` script which:
1. Downloads the `backlog.json` file from the Azure storage container
2. Processes the MDF files listed in the backlog
3. Converts them to Parquet format

## Environment Variables

The container requires the following environment variables:

| Variable | Description |
|----------|-------------|
| `INPUT_BUCKET` | Azure blob storage container name containing the backlog.json and MDF files |
| `StorageConnectionString` | Azure Storage connection string |
| `OUTPUT_BUCKET` | (Optional) Override for output container, defaults to INPUT_BUCKET |

## Deployment in Azure

### Using Azure Container Apps Jobs

```hcl
resource "azurerm_container_app_job" "mdftoparquet_job" {
  name                     = "mdftoparquet-processor"
  container_app_environment_id = azurerm_container_app_environment.environment.id
  resource_group_name      = azurerm_resource_group.example.name
  
  template {
    containers {
      name  = "mdftoparquet"
      image = "ghcr.io/css-electronics/canedge-mdftoparquet-automation:latest"
      
      env {
        name  = "INPUT_BUCKET"
        value = "your-storage-container"
      }
      
      env {
        name  = "StorageConnectionString"
        secret_name = "storage-connection-string"
      }
    }
    
    min_replicas    = 1
    max_replicas    = 1
  }
  
  secrets {
    name  = "storage-connection-string"
    value = "your-storage-connection-string"
  }
}
```

## Local Testing

You can test this container locally by building and running it with Docker:

```bash
docker build -t mdftoparquet-azure .
docker run --rm -e INPUT_BUCKET=your-container -e StorageConnectionString="your-connection-string" mdftoparquet-azure
```

## Building and Publishing

The container is automatically built and published to GitHub Container Registry using GitHub Actions. See the `.github/workflows/publish-container.yml` file in the repository root for details.
