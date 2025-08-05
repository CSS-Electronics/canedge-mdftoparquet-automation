from datetime import datetime, timedelta


# -------------------------------------------
# === Define class for aggregation ===
class AggregateDataLake:
    def __init__(
        self,
        s3,
        bucket_name,
        aggregations_file,
        aggregations_folder,
        table_name,
        trip_gap_min,
        trip_min_length_min,
        start_date,
        end_date,
    ):
        import sys
        import boto3
        from botocore.exceptions import NoCredentialsError, ClientError

        self.s3 = s3
        self.bucket_name = bucket_name
        self.aggregations_file = aggregations_file
        self.aggregations_folder = aggregations_folder
        self.trip_gap_min = trip_gap_min
        self.trip_min_length_min = trip_min_length_min
        self.table_name = table_name
        self.start_date = start_date
        self.end_date = end_date

        # initialize s3 if required
        s3_client = None
        if self.s3:
            try:
                s3_client = boto3.client("s3")
                if self.bucket_name:
                    s3_client.list_objects_v2(Bucket=bucket_name, MaxKeys=1)
                    print(f"Connection to bucket '{bucket_name}' successful.")
            except NoCredentialsError:
                print("No AWS credentials found - exiting.")
            except ClientError as e:
                if e.response["Error"]["Code"] == "AccessDenied":
                    print("Access to the bucket is denied - exiting.")
                elif e.response["Error"]["Code"] == "NoSuchBucket":
                    print("Bucket does not exist  - exiting.")
                else:
                    print(f"Error occurred - exiting: {e}")
            except Exception as e:
                print(f"Error occurred - exiting: {e}")
            if s3_client == None:
                sys.exit()

        self.s3_client = s3_client

    # Load aggregations JSON file from S3 or local disk
    def load_aggregation_json(self):
        import json, sys

        config = None
        if self.s3:
            try:
                response = self.s3_client.get_object(
                    Bucket=self.bucket_name, Key=self.aggregations_file
                )
                json_data = response["Body"].read()
                config = json.loads(json_data)
            except Exception as e:
                print(f"Error loading JSON from S3 - exiting: {e}")
        else:
            try:
                with open(
                    "/".join([self.bucket_name, self.aggregations_file]), "r"
                ) as file:
                    config = json.load(file)
            except Exception as e:
                print(f"Error loading JSON from local disk - exiting: {e}")

        if config == None:
            sys.exit()

        print("\nData aggregation config loaded: \n\n", json.dumps(config), "\n")
        return config

    # Function to extract all device IDs
    def get_all_device_ids(self):
        import os, re

        device_ids = []

        if self.s3:
            # List all objects in the bucket and filter by prefix and delimiter to get device IDs
            paginator = self.s3_client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.bucket_name, Delimiter="/"):
                device_ids.extend(
                    [
                        prefix["Prefix"].rstrip("/").split("/")[-1]
                        for prefix in page.get("CommonPrefixes", [])
                    ]
                )
        else:
            # List all directories in the local bucket_name directory
            local_path = self.bucket_name
            if os.path.exists(local_path):
                device_ids = next(os.walk(local_path))[1]  # Directories only

        # filter to only include valid device IDs
        device_ids = [s for s in device_ids if (re.match(r"^[0-9A-F]{8}$", s))]

        return device_ids

    # List Parquet files from S3 or local disk
    def list_parquet_files(self, prefix):
        import os

        if self.s3:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name, Prefix=prefix
            )
            files = [
                obj["Key"]
                for obj in response.get("Contents", [])
                if obj["Key"].endswith(".parquet")
            ]
            return files
        else:
            local_path_full = "/".join([self.bucket_name, prefix])
            if os.path.exists(local_path_full) == False:
                return []
            files = [
                "/".join([local_path_full, f])
                for f in os.listdir(local_path_full)
                if f.endswith(".parquet")
            ]
            return files

    # Function for downloading S3 files to local temp directory
    def get_parquet_files_s3(self, files, temp_dir):
        import os

        local_files = []
        for file_key in files:
            local_file_path = os.path.join(temp_dir, os.path.basename(file_key))
            local_files.append(local_file_path)
            response = self.s3_client.download_file(
                self.bucket_name, file_key, local_file_path
            )
        files = local_files
        return files

    # Function for extracting trip windows
    def get_trip_windows(self, trip_path):
        import pyarrow.parquet as pq
        import pandas as pd
        import tempfile

        # list Parquet files in trip directory from S3 or local disk
        files = self.list_parquet_files(trip_path)
        if len(files) == 0:
            return []

        # Load Parquet files into dataframes and concatenate. If s3, first download Parquet files to temp dir
        with tempfile.TemporaryDirectory() as temp_dir:
            if self.s3:
                files = self.get_parquet_files_s3(files, temp_dir)
            dfs = [pq.read_table(f).to_pandas() for f in files]

        df = pd.concat(dfs, ignore_index=True)

        df["t"] = pd.to_datetime(df["t"])
        df["time_diff"] = df["t"].diff()

        # Identify trip starts
        trip_starts = df[df["time_diff"] > pd.Timedelta(minutes=self.trip_gap_min)][
            "t"
        ].tolist()
        if df["t"].iloc[0] not in trip_starts:
            trip_starts.insert(0, df["t"].iloc[0])

        # Identify trip ends
        trip_ends = []
        for i in range(len(trip_starts) - 1):
            # Find the last timestamp before the next trip starts
            last_time_before_next_trip = df[df["t"] < trip_starts[i + 1]]["t"].iloc[-1]
            trip_ends.append(last_time_before_next_trip)

        # Handle the last trip end
        trip_ends.append(df["t"].iloc[-1])

        # Filter out trips that are shorter than the minimum duration
        trip_windows = [
            (start, end)
            for start, end in zip(trip_starts, trip_ends)
            if (end - start) >= pd.Timedelta(minutes=self.trip_min_length_min)
        ]

        return trip_windows

    # Function for creating a date range
    def daterange(self):
        from datetime import timedelta

        for n in range(int((self.end_date - self.start_date).days) + 1):
            yield self.start_date + timedelta(n)

    # Function for processing a single message for a trip
    def process_aggregation_for_trip(
        self,
        device_id,
        message,
        signals,
        aggregation_types,
        trip_window,
        cluster_name,
        df
    ):
        df = df[(df["t"] >= trip_window[0]) & (df["t"] <= trip_window[1])]
        results = []
        if df.empty:
            return results

        for signal in signals:
            if signal not in df.columns:
                continue
            
            for aggregation_type in aggregation_types:
                result_value = None
                if aggregation_type == "avg":
                    result_value = df[signal].mean()
                elif aggregation_type == "median":
                    result_value = df[signal].median()
                elif aggregation_type == "max":
                    result_value = df[signal].max()
                elif aggregation_type == "min":
                    result_value = df[signal].min()
                elif aggregation_type == "sum":
                    result_value = df[signal].sum()
                elif aggregation_type == "first":
                    result_value = df[signal].iloc[0]
                elif aggregation_type == "last":
                    result_value = df[signal].iloc[-1]
                elif aggregation_type == "delta_sum":
                    result_value = df[signal].diff().sum()
                elif aggregation_type == "delta_sum_pos":
                    delta = df[signal].diff()
                    result_value = delta[delta > 0].sum()
                elif aggregation_type == "delta_sum_neg":
                    delta = df[signal].diff()
                    result_value = delta[delta < 0].sum()

                count = df[signal].count()
                duration = (df["t"].max() - df["t"].min()).total_seconds()

                trip_id = f"{device_id}_{(trip_window[0].strftime('%Y%m%dT%H%M%S.%f'))}"
                trip_start = trip_window[0]
                trip_end = trip_window[1]

                if result_value is not None:
                    results.append(
                        [
                            device_id,
                            message,
                            signal,
                            aggregation_type,
                            result_value,
                            count,
                            duration,
                            trip_start,
                            trip_end,
                            trip_id,
                            cluster_name,
                        ]
                    )

        return results

    # Function for writing the results from one day to a Parquet file
    def write_results_to_parquet(self, results, date):
        import pyarrow.parquet as pq
        import pandas as pd
        import pyarrow as pa
        import tempfile
        import os

        if not results:
            return

        df = pd.DataFrame(
            results,
            columns=[
                "DeviceID",
                "Message",
                "Signal",
                "Aggregation",
                "SignalValue",
                "SignalCount",
                "Duration",
                "TripStart",
                "TripEnd",
                "TripID",
                "Cluster",
            ],
        )
        schema = pa.schema(
            [
                ("DeviceID", pa.string()),
                ("Message", pa.string()),
                ("Signal", pa.string()),
                ("Aggregation", pa.string()),
                ("SignalValue", pa.float64()),
                ("SignalCount", pa.int64()),
                ("Duration", pa.float64()),
                ("TripStart", pa.timestamp("us")),
                ("TripEnd", pa.timestamp("us")),
                ("TripID", pa.string()),
                ("Cluster", pa.string()),
            ]
        )
        date_path = date.strftime("%Y/%m/%d")

        if self.s3:
            # Create a temporary file
            with tempfile.NamedTemporaryFile(
                delete=False, suffix=".parquet"
            ) as tmp_file:
                # Write DataFrame to Parquet file locally
                pq.write_table(pa.Table.from_pandas(df, schema=schema), tmp_file.name)

                # Upload the file to S3
                s3_path = f"{self.aggregations_folder}/{self.table_name}/{date_path}/{date.strftime('%Y%m%d')}.parquet"
                self.s3_client.upload_file(tmp_file.name, self.bucket_name, s3_path)
                print(
                    f"+ stored aggregation Parquet on S3 | {len(results)} rows | {s3_path}"
                )
        else:
            local_path = f"{self.bucket_name}/{self.aggregations_folder}/{self.table_name}/{date_path}"
            os.makedirs(local_path, exist_ok=True)
            file_path = f"{local_path}/{date.strftime('%Y%m%d')}.parquet"
            pq.write_table(pa.Table.from_pandas(df, schema=schema), file_path)
            print(
                f"+ stored aggregation Parquet locally | {len(results)} rows | {file_path}"
            )

    # Function for extracting cluster details from config
    def get_cluster_detail(self, config, cluster):
        cluster_detail = next(
            (d for d in config["cluster_details"] if cluster in d["clusters"]),
            None,
        )
        return cluster_detail["details"]

    # Function for processing a single device
    def process_single_device(
        self, cluster, device_id, cluster_detail, cluster_aggregations, date_path
    ):
        import tempfile
        import pyarrow.parquet as pq
        import pandas as pd
        
        device_results = []

        # identify trip windows for the device & day based on the cluster trip message
        trip_message = cluster_detail["trip_identifier"].get("message", "")
        if trip_message == "":
            return []

        trip_path = "/".join([device_id, trip_message, date_path])
        trip_windows = self.get_trip_windows(trip_path)
        if len(trip_windows) == 0:
            return []

        # extract data aggregation values per trip and add to daily_results
        for agg in cluster_aggregations:
            agg_path = "/".join([device_id, agg["message"], date_path])
            
            
            # list files in message directory path on S3 or local disk
            files = self.list_parquet_files(agg_path)
            if len(files) == 0:
                print(f"No Parquet files found for {agg} - skipping")
                continue


            # if s3, download files to temp dir
            with tempfile.TemporaryDirectory() as temp_dir:
                if self.s3:
                    files = self.get_parquet_files_s3(files, temp_dir)
                dfs = [pq.read_table(f).to_pandas() for f in files]

            df = pd.concat(dfs, ignore_index=True)
            
            for trip_window in trip_windows:
                agg_results = self.process_aggregation_for_trip(
                    device_id,
                    agg["message"],
                    agg["signal"],
                    agg["aggregation"],
                    trip_window,
                    cluster,
                    df
                )

                if agg_results:
                    device_results.extend(agg_results)
        return device_results

    # Function for overall processing of the data lake
    def process_data_lake(self, config):
        from concurrent.futures import ThreadPoolExecutor, as_completed

        days_processed = 0
        total_days = (self.end_date - self.start_date).days + 1
        print("Processing:")

        # process each day in range
        for i, single_date in enumerate(self.daterange(), start=1):
            print(f"\n- date {i}/{total_days}: {single_date}")
            daily_results = []
            date_path = single_date.strftime("%Y/%m/%d")

            # process each cluster of devices and extract the aggregation details
            for device_cluster in config["device_clusters"]:
                cluster = device_cluster["cluster"]
                cluster_detail = self.get_cluster_detail(config, cluster)
                if cluster_detail == None:
                    continue
                cluster_aggregations = cluster_detail.get(self.aggregations_folder, [])
                print(f"-- cluster: {cluster}")

                # process each device in cluster
                for device_id in device_cluster["devices"]:
                    print(f"--- device: {device_id}")
                    device_results = []
                    device_results = self.process_single_device(
                        cluster,
                        device_id,
                        cluster_detail,
                        cluster_aggregations,
                        date_path,
                    )

                    if device_results:
                        daily_results.extend(device_results)

            if daily_results:
                days_processed += 1
                self.write_results_to_parquet(daily_results, single_date)
            else:
                print("- no data extracted")

        print(
            f"\nStored {days_processed} days with data across {total_days} days from {self.start_date} to {self.end_date}"
        )


# Get s3 bucket name via AWS Glue arguments if using Glue
def get_bucket_name_glue():
    from awsglue.utils import getResolvedOptions
    import sys

    args = getResolvedOptions(sys.argv, ["bucket_output"])
    return args["bucket_output"]


# -------------------------------------------
# === Main script ===

# Set s3 to True to use S3, False for local file system
s3 = True

# If you deploy the script locally instead of via AWS Glue, specify the bucket_name
# If S3, specify the S3 data lake bucket name - if local, specfy the local folder name
# If deployment via AWS Glue, the bucket name does not matter (it is overwritten below)
bucket_name = "css-electronics-dashboard-playground-parquet"

try:
    bucket_name = get_bucket_name_glue()
except:
    pass

# Define aggregations config file name (store in root of your S3 bucket or local_folder_name)
aggregations_file = "aggregations.json"

# Define aggregations folder name and output table name [we recommend leaving these unchanged]
aggregations_folder = "aggregations"
table_name = "tripsummary"

# Define size of gap (#min) that determines when a new trip occurs and the minimum trip window length
trip_gap_min = 10
trip_min_length_min = 1

# Define the time period for data processing (default is the full day yesterday)
start_date = datetime.today() - timedelta(days=1)
end_date = start_date

# Alternatively specify a specific date or time period, e.g. for testing or historical backlog processing
# start_date = datetime(2022, 3, 26)
# end_date = datetime.today()


# -------------------------------------------
# Initialize aggregation class
aggr = AggregateDataLake(
    s3,
    bucket_name,
    aggregations_file,
    aggregations_folder,
    table_name,
    trip_gap_min,
    trip_min_length_min,
    start_date,
    end_date,
)

# Load aggregation configuration file
config = aggr.load_aggregation_json()

# Process the data lake
aggr.process_data_lake(config)