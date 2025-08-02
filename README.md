# Multi-Cloud MDF to Parquet Converter

This repository contains cloud functions for automatically decoding CANedge MDF log files to Parquet files across Amazon AWS, Google Cloud, and Microsoft Azure platforms. These functions are triggered when new MDF files are uploaded to a cloud storage bucket, process them using DBC decoding, and store the results in Parquet format in an output bucket.

## Repository Structure

The repository is organized into three main directories:

### 1. `mdftoparquet/` - Single File Processing

Contains the cloud function code for processing individual MDF files when they are uploaded to cloud storage.

#### Cloud Function Entry Points
- `lambda_function.py` - AWS Lambda function entry point
- `main.py` - Google Cloud Function entry point
- `function_app.py` - Azure Function entry point

#### Cloud-Specific Configuration
- `azure-function-root/` - Azure function configuration files
- `google-function-root/` - Google Cloud Function configuration files

### 2. `mdftoparquet-backlog/` - Backlog Processing

Contains scripts for batch processing existing MDF files in cloud storage buckets.

- `process_backlog_amazon.py` - Script for processing backlog files in AWS S3
- `process_backlog_azure.py` - Script for processing backlog files in Azure Blob Storage
- `process_backlog_google.py` - Script for processing backlog files in Google Cloud Storage

### 3. `local-testing/` - Local Testing Environment

Contains utilities and scripts for local development and testing.

- `run_test.py` - Main test runner for simulating cloud function invocation locally
- `local_invocation.py` - Utility for invoking cloud functions with proper environment setup
- `utils_testing.py` - Utility functions for creating test events and loading credentials

### Core Modules
Shared functionality is in the `modules/` directory at the repository root:
- `mdf_to_parquet.py` - Main processing logic for converting MDF to Parquet
- `cloud_functions.py` - Cloud provider specific operations (download/upload/list files)
- `utils.py` - Utility functions for decoding, custom messages, and event detection
- `custom_message_functions.py` - Functions for creating calculated signals
- `functions.py` - Additional helper functions

### Decoders
- `mdf2parquet_decode` - Linux executable for DBC decoding MDF files to Parquet (used in cloud environments)
- `mdf2parquet_decode.exe` - Windows executable for DBC decoding (used for local testing)

## Credential Files

Credential files for local testing are stored in the `local-testing/creds/` directory with one file per cloud provider:
- `local-testing/creds/amazon-creds.json` - AWS credentials
- `local-testing/creds/google-creds.json` - Google Cloud service account key
- `local-testing/creds/azure-creds.json` - Azure storage connection strings

These credential files are used during local testing and should contain the necessary permissions to:
- Read from the input bucket/container
- Write to the output bucket/container
- Access notification services (SNS for AWS)

### Credentials Format

Each cloud provider requires a specific credential file format:

1. **Amazon AWS (`amazon-creds.json`):**
```json
{
    "AWS_ACCESS_KEY_ID": "AKIAXXXXXXXXXXXXXXXX",
    "AWS_SECRET_ACCESS_KEY": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
    "AWS_DEFAULT_REGION": "eu-central-1",
    "SNS_ARN": "arn:aws:sns:eu-central-1:123456789012:mdf-to-parquet-lambda-event-sns"
}
```

2. **Microsoft Azure (`azure-creds.json`):**
```json
{
    "STORAGE_CONNECTION_STRING": "DefaultEndpointsProtocol=https;AccountName=yourstorageaccount;AccountKey=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx;EndpointSuffix=core.windows.net"
}
```

3. **Google Cloud (`google-creds.json`):**
```json
{
    "type": "service_account",
    "project_id": "your-project-id",
    "private_key_id": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
    "private_key": "-----BEGIN PRIVATE KEY-----\nxxx...xxx\n-----END PRIVATE KEY-----\n",
    "client_email": "service-account-name@your-project-id.iam.gserviceaccount.com",
    "client_id": "123456789012345678901",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/service-account-name%40your-project-id.iam.gserviceaccount.com",
    "universe_domain": "googleapis.com"
}
```

#### How to Find Credential Information

- **Google credentials**: The service account key can be found in your input bucket and is named `<id>-service-account-key.json`
- **Amazon credentials**: The access information can be found in the CloudFormation stack outputs (at the bottom of the outputs)
- **Azure credentials**: The storage connection string can be found in the Azure Function/Settings/Environment variables under `StorageConnectionString`


## Local Testing

The repository includes several scripts and batch files in the `local-testing/` directory that allow you to test the cloud functions locally before deployment:

### Environment Setup

1. **Credential Files**:
   - Place the appropriate credentials in `local-testing/creds/` directory
   - Each cloud provider requires its own credential file format

2. **Environment Variables**:
   - `INPUT_BUCKET` - Source bucket/container name where MDF files are stored
   - `MF4_DECODER` - Path to the decoder executable (automatically set by test scripts)
   - Cloud-specific environment variables are set automatically by the `run_test.py` script

### Testing Single File Processing

Use these batch files to test processing a single MDF file:

```
.\test_amazon_single.bat   # Test AWS Lambda single file processing
.\test_azure_single.bat    # Test Azure Function single file processing
.\test_google_single.bat   # Test Google Cloud Function single file processing
```

These scripts will:
1. Load appropriate credentials
2. Create mock cloud events
3. Set necessary environment variables
4. Invoke the cloud function code locally

### Testing Backlog Processing

Use these batch files to test batch processing with a backlog.json file:

```
.\test_amazon_backlog.bat  # Test AWS S3 backlog processing
.\test_azure_backlog.bat   # Test Azure Blob Storage backlog processing
.\test_google_backlog.bat  # Test Google Cloud Storage backlog processing
.\test_backlog_local.bat   # Test local backlog processing
```

### Under the Hood

- `run_test.py` - Main test runner script that handles environment setup, credentials loading, and cloud function invocation
- `utils_testing.py` - Contains utilities for loading credentials and creating mock cloud events for different cloud providers
   - Sets necessary environment variables (e.g., `GOOGLE_APPLICATION_CREDENTIALS` for Google Cloud)
   - Creates a test event based on the cloud provider or uses the backlog file if provided
   - Invokes the appropriate cloud function entry point with the test event
   - Processes the files similar to how they would be processed in the cloud environment

4. **Batch Processing with backlog.json**:
   - The script can process a list of files using a backlog.json file
   - Create a JSON file with an array of file paths to process:
     ```json
     [
       "00000005-6497EEED.MF4",
       "00000006-6497F67D.MF4"
     ]
     ```
   - Run with the backlog option and specify the input bucket:
     ```
     python local_invocation.py --cloud Amazon --backlog backlog.json --input-bucket my-bucket-name
     ```

## Local Backlog Processing

The repository includes a local testing framework that allows you to run the cloud functions locally:

```bash
python local-testing/run_test.py \
  --cloud Local \
  --input-bucket <path-to-input-folder> \
  --backlog
```

This will process a backlog of files stored in a local folder without requiring any cloud credentials.

## Backlog Processing

To process a large number of MDF files efficiently, backlog processing scripts are provided for each cloud provider and local environments:
- `mdftoparquet-backlog/process_backlog_amazon.py`
- `mdftoparquet-backlog/process_backlog_azure.py`
- `mdftoparquet-backlog/process_backlog_google.py`
- `mdftoparquet-backlog/process_backlog_local.py`

These scripts read a `backlog.json` file from the root of the input bucket or folder, which contains a list of MDF files to process.

### Backlog File Structure

The backlog file is structured as a flat list of items to process:

```json
[
  "2F6913DB/",
  "ABCDEF12/00000088/",
  "2F6913DB/00000086/00000001-62961868.MF4",
  "2F6913DB/00000086/00000003-62977DFB.MF4"
]
```

The system will handle grouping and batching these items for optimal processing.

### Prefix Types and File Paths

You can specify three types of entries in the backlog file:

1. **Device Prefixes**: Path to a device folder (e.g., `"2F6913DB/"`) - processes all sessions under that device
2. **Session Prefixes**: Path to a specific session (e.g., `"2F6913DB/00000088/"`) - processes all files in that session
3. **Individual Files**: Complete file path (e.g., `"2F6913DB/00000086/00000001-62961868.MF4"`) - processes a specific file

**Note**: For prefixes (device or session), trailing slashes are optional - they will be automatically added if missing. File paths should not have trailing slashes.

### Processing Backlog Files

To process a backlog of files, use the `--backlog` flag:

```bash
python local-testing/run_test.py \
  --cloud <Amazon|Google|Azure|Local> \
  --input-bucket <bucket-name-or-folder> \
  --backlog
```

The backlog feature expects a `backlog.json` file in the root of the input bucket or folder, containing a list of MDF files to process.

## Deployment

### Preparing Deployment Packages

The repository includes a script to generate deployment ZIP files for each cloud platform:

```
cd mdftoparquet
.\prepare_mdftoparquet_zip_files.bat
```

This script will:

1. Check for the presence of 7-Zip and install it if necessary
2. Read version information from `mdftoparquet-versions.cfg`
3. Create separate ZIP files for each cloud provider in the `release/` directory:
   - `mdf-to-parquet-amazon-function-v{version}.zip` for AWS Lambda
   - `mdf-to-parquet-google-function-v{version}.zip` for Google Cloud Functions
   - `mdf-to-parquet-azure-function-v{version}.zip` for Azure Functions

Each ZIP file contains:
- The appropriate cloud function entry point
- The Linux version of the decoder executable (`mdf2parquet_decode`)
- All required modules from the `modules/` directory
- Cloud-specific configuration files from the respective function root directories

### Deployment to Cloud Platforms

#### Amazon AWS Lambda

1. Upload the Amazon ZIP file to your AWS Lambda function:
   - AWS Console > Lambda > Functions > Your Function > Code > Upload from > .zip file
   - Select the generated ZIP file from the `release/` directory

2. Configure environment variables:
   - `INPUT_BUCKET` - S3 bucket containing MDF files
   - Make sure the execution role has permissions to read from the input bucket and write to the output bucket

3. Set up an S3 trigger for new object creation events:
   - Add trigger > S3 > Select your input bucket > Event type: All object create events

#### Google Cloud Functions

1. Deploy the Google ZIP file to Google Cloud Functions:
   - Google Cloud Console > Cloud Functions > Create Function
   - Runtime: Python 3.9+
   - Entry point: `process_mdf_file`
   - Upload the generated ZIP file from the `release/` directory

2. Configure environment variables:
   - `INPUT_BUCKET` - GCS bucket containing MDF files
   - Make sure the service account has permissions to read/write to the appropriate buckets

3. Set up a Cloud Storage trigger:
   - Trigger type: Cloud Storage
   - Event type: Finalize/Create
   - Bucket: Your input bucket

#### Azure Functions

1. Deploy the Azure ZIP file to Azure Functions:
   - Azure Portal > Function Apps > Your Function App > Functions
   - Deploy the generated ZIP file using Azure Functions Core Tools or VS Code

2. Configure application settings:
   - `INPUT_BUCKET` - Azure Storage container containing MDF files
   - Configure storage connection strings in Application Settings

3. Set up a Blob Storage trigger:
   - Binding type: Blob Storage trigger
   - Path: your-container/{name}

### Backlog Processing

For processing existing files in bulk:

1. Create a `backlog.json` file with an array of file paths to process
2. Upload this file to the root of your input bucket
3. Use the appropriate backlog processing script for your cloud provider:
   - AWS: Use the `process_backlog_amazon.py` script
   - Azure: Use the `process_backlog_azure.py` script
   - Google: Use the `process_backlog_google.py` script

## Parquet File Validation

To validate the generated Parquet files, you can use the provided `validate_parquet_files.py` script:

```
python validate_parquet_files.py <directory_path>
```

This utility will:
1. Recursively scan for all Parquet files (*.parquet, *.pq) in the specified directory
2. Attempt to load each file using PyArrow to verify its validity
3. Report any invalid Parquet files along with detailed error messages
4. Provide a summary of valid and invalid files

## How It Works

1. **Trigger**: A new MDF file (`.MF4`, `.MFC`, `.MFE`, or `.MFM`) is uploaded to the input bucket/container
2. **Processing**:
   - The cloud function is triggered
   - The MDF file and necessary DBC files are downloaded to the function's temporary storage
   - `mdf2parquet_decode` converts the MDF data to Parquet format using DBC decoding
   - Custom message calculations are performed if configured
   - Event detection is performed if configured
3. **Output**: 
   - Parquet files are created for each unique CAN message in the structure: `<deviceid>/<message>/<yyyy>/<mm>/<dd>/<xyz>.parquet`
   - Files are uploaded to the output bucket/container (named with `-parquet` suffix)
   - For events, notifications may be sent (e.g., via SNS in AWS)

## Additional Configuration

- **Custom Messages**: Define custom calculated signals by uploading a `custom-messages.json` file to the input bucket
- **Event Detection**: Configure event detection by uploading an `events.json` file to the input bucket
- **Device-Specific DBC**: Device-specific DBC files can be referenced in a device configuration file

## Notes

- The code is designed to be as cloud-agnostic as possible, with 95% of the code shared between the cloud functions
- The MDF decoder executables (`mdf2parquet_decode` and `mdf2parquet_decode.exe`) are pre-compiled and should be included in the deployment packages
- When testing locally on Windows, use the `.exe` version of the decoder
- When deploying to cloud environments, use the Linux version of the decoder

## Using Backlog Files

For batch processing of multiple files, you can use a `backlog.json` file. This allows processing existing files in the storage bucket without re-uploading them.

### Backlog File Structure

The backlog file is structured as a flat list of items to process:

```json
[
  "2F6913DB/",
  "ABCDEF12/00000088/",
  "2F6913DB/00000086/00000001-62961868.MF4",
  "2F6913DB/00000086/00000003-62977DFB.MF4"
]
```

The system will handle grouping and batching these items for optimal processing.

### Prefix Types and File Paths

You can specify three types of entries in the backlog file:

1. **Device Prefixes**: Path to a device folder (e.g., `"2F6913DB/"`) - processes all sessions under that device
2. **Session Prefixes**: Path to a specific session (e.g., `"2F6913DB/00000088/"`) - processes all files in that session
3. **Individual Files**: Complete file path (e.g., `"2F6913DB/00000086/00000001-62961868.MF4"`) - processes a specific file

**Note**: For prefixes (device or session), trailing slashes are optional - they will be automatically added if missing. File paths should not have trailing slashes.

### Running with a Backlog File

To process a backlog file:

```
python local_invocation.py --cloud Amazon --backlog backlog-examples/backlog.json --input-bucket your-bucket-name
```

The system will:
1. Expand all prefixes into individual file paths
2. Group files by session to optimize processing
3. Execute the processing in batches
4. Avoid duplicate processing if the same file appears multiple times
