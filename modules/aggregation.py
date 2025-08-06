from datetime import datetime, timedelta
import os
import json
import logging
import pandas as pd
from pathlib import Path
import sys
import tempfile
from modules.cloud_functions import upload_object, list_objects, download_object, list_objects_with_pagination
from modules.utils import DownloadObjects

class AggregateData:
    """
    Class for data aggregation from Parquet files. Cloud agnostic implementation.
    Aggregates data into trip-based averages based on configuration in aggregations.json.
    
    Args:
        cloud (str): Cloud provider ("Amazon", "Google", "Azure", or "Local")
        client: Cloud storage client
        input_bucket (str): Name of the input bucket/container (for aggregations.json)
        output_bucket (str): Name of the output bucket/container (for Parquet files and results)
        aggregations_file (str): Name of the aggregations config file
        aggregations_folder (str): Folder name for aggregations
        table_name (str): Name of the output table
        logger: Logger object for logging messages
        
    """

    def __init__(
        self,
        cloud,
        client,
        input_bucket,
        output_bucket,
        aggregations_file="aggregations.json",
        aggregations_folder="aggregations",
        table_name="tripsummary",
        logger=None
    ):
        self.cloud = cloud
        self.client = client
        self.input_bucket = input_bucket
        self.output_bucket = output_bucket
        self.aggregations_file = aggregations_file
        self.aggregations_folder = aggregations_folder
        self.table_name = table_name
        self.logger = logger
            
    def load_aggregation_json(self):
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                do = DownloadObjects(
                    self.cloud,
                    self.client,
                    self.input_bucket,
                    Path(temp_dir),
                    "", 
                    self.logger
                )
                
                config = do.download_json_file(self.aggregations_file)
                self.logger.info(f"Aggregation Configuration File: {config}")
                self._extract_config_parameters(config)
                
                return config
                
        except Exception as e:
            self.logger.error(f"Error loading JSON file: {e}")
            return None
            
    def _extract_config_parameters(self, config):
        # Extract trip parameters
        if 'config' in config and 'trip' in config['config']:
            trip_config = config['config']['trip']
            
            if 'trip_gap_min' in trip_config:
                self.trip_gap_min = trip_config['trip_gap_min']
                
            if 'trip_min_length_min' in trip_config:
                self.trip_min_length_min = trip_config['trip_min_length_min']
    
        # Extract date parameters - ensure proper configuration is provided
        if not ('config' in config and 'date' in config['config']):
            raise ValueError("Missing required 'date' configuration in aggregation config")
        
        date_config = config['config']['date']
        date_mode = date_config.get('mode')
        
        if not date_mode:
            raise ValueError("Missing required 'mode' in date configuration")
        
        if date_mode == 'previous_day':
            # Use yesterday as the date range
            self.start_date = datetime.today() - timedelta(days=1)
            self.end_date = self.start_date
        elif date_mode == 'specific_period':
            # Parse start_date and end_date from config - no defaults allowed
            if 'start_date' not in date_config:
                raise ValueError("For 'specific_period' mode, 'start_date' must be explicitly specified")
            if 'end_date' not in date_config:
                raise ValueError("For 'specific_period' mode, 'end_date' must be explicitly specified")
            
            start_date_str = date_config['start_date']
            end_date_str = date_config['end_date']
            
            # Ensure date strings are not empty
            if not start_date_str or not end_date_str:
                raise ValueError("For 'specific_period' mode, dates cannot be empty strings")
            
            try:
                self.start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
                self.end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
                
                # Validate date range
                if self.end_date < self.start_date:
                    raise ValueError(f"Invalid date range: end_date {end_date_str} is before start_date {start_date_str}")
            except ValueError as e:
                raise ValueError(f"Invalid date format in configuration. Dates must be in 'YYYY-MM-DD' format: {e}")
        else:
            raise ValueError(f"Unknown date mode: {date_mode}. Supported modes: 'previous_day', 'specific_period'")

    def list_parquet_files(self, prefix):
        files = []
        try:
            result = list_objects_with_pagination(self.cloud, self.client, self.output_bucket, self.logger, prefix=prefix, supress=True)
            files = [obj["name"] for obj in result.get("objects", []) if obj["name"].endswith(".parquet")]
        except Exception as e:
            self.logger.error(f"Error listing Parquet files: {e}")
        
        return files

    def get_parquet_files(self, files, temp_dir):
        local_files = []
        for file_path in files:
            local_path = os.path.join(temp_dir, os.path.basename(file_path))
            success = download_object(
                self.cloud, 
                self.client, 
                self.output_bucket, 
                file_path, 
                local_path,
                self.logger,
                True
            )
            
            if success:
                local_files.append(local_path)
            else:
                self.logger.error(f"Failed to download {file_path}")
                
        return local_files

    # Function for extracting trip windows
    def get_trip_windows(self, trip_path):
        import pyarrow.parquet as pq
        
        try:
            # List Parquet files in trip directory
            files = self.list_parquet_files(trip_path)
            if len(files) == 0:
                return []
                
            # Download files and load into dataframes
            with tempfile.TemporaryDirectory() as temp_dir:
                local_files = self.get_parquet_files(files, temp_dir)
                if not local_files:
                    return []
                    
                dfs = [pq.read_table(f).to_pandas() for f in local_files]
                if not dfs:
                    return []
                    
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
            
        except Exception as e:
            # self.logger.error(f"--- Error identifying trip windows: {e}")
            return []
   
    # Function for creating a date range
    def daterange(self):
        for n in range((self.end_date - self.start_date).days + 1):
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

        start_str = trip_window[0].strftime("%Y-%m-%d %H:%M:%S")
        end_str = trip_window[1].strftime("%Y-%m-%d %H:%M:%S")
        self.logger.info(f"---- Processing trip from {start_str} to {end_str} for device {device_id}, message {message}")
  
        for signal in signals:
            if signal not in df.columns:
                continue
                
            # Calculate aggregations for this signal
            for agg_type in aggregation_types:
                value = None
                if agg_type == "avg":
                    value = df[signal].mean()
                elif agg_type == "median":
                    value = df[signal].median()
                elif agg_type == "max":
                    value = df[signal].max()
                elif agg_type == "min":
                    value = df[signal].min()
                elif agg_type == "sum":
                    value = df[signal].sum()
                elif agg_type == "first":
                    value = df[signal].iloc[0]
                elif agg_type == "last":
                    value = df[signal].iloc[-1]
                elif agg_type == "delta_sum":
                    value = df[signal].diff().sum()
                elif agg_type == "delta_sum_pos":
                    delta = df[signal].diff()
                    value = delta[delta > 0].sum()
                elif agg_type == "delta_sum_neg":
                    delta = df[signal].diff()
                    value = delta[delta < 0].sum()
                else:
                    self.logger.warning(f"---- Unsupported aggregation type: {agg_type}")
                    continue
                    
                # Calculate count and duration like the original implementation
                count = df[signal].count()
                duration = (df["t"].max() - df["t"].min()).total_seconds()

                # Create a trip ID using the same format as original
                trip_id = f"{device_id}_{(trip_window[0].strftime('%Y%m%dT%H%M%S.%f'))}"
                trip_start = trip_window[0]
                trip_end = trip_window[1]
        
                # Append result as a list in the same order as the columns
                if value is not None:
                    results.append(
                        [
                            device_id,
                            message,
                            signal,
                            agg_type,
                            float(value),
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
        import pyarrow as pa
        
        if not results:
            self.logger.info("No results to write")
            return False
            
        try:
            # Create DataFrame with explicit column names matching the original implementation
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
            
            # Construct date path like the original
            date_path = date.strftime("%Y/%m/%d")
            
            if self.cloud == "Local":
                local_path = f"{self.output_bucket}/{self.aggregations_folder}/{self.table_name}/{date_path}"
                os.makedirs(local_path, exist_ok=True)
                file_path = f"{local_path}/{date.strftime('%Y%m%d')}.parquet"
                pq.write_table(pa.Table.from_pandas(df, schema=schema), file_path)
                self.logger.info(f"- Stored aggregation Parquet locally | {len(results)} rows | {file_path}")
                return True
            else:
                # For cloud storage, use a temp file and upload
                tmp_file_path = None
                try:
                    # Create a named temporary file
                    with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as tmp_file:
                        tmp_file_path = tmp_file.name
                    
                    # Write DataFrame to Parquet file locally with the schema
                    # We do this after closing the file to avoid keeping it open
                    pq.write_table(pa.Table.from_pandas(df, schema=schema), tmp_file_path)
                    
                    # Define cloud path
                    cloud_path = f"{self.aggregations_folder}/{self.table_name}/{date_path}/{date.strftime('%Y%m%d')}.parquet"
                    
                    # Upload to cloud storage
                    success = upload_object(
                        self.cloud,
                        self.client,
                        self.output_bucket,
                        cloud_path,
                        tmp_file_path,
                        self.logger
                    )
                    
                    if success:
                        self.logger.info(f"- Stored aggregation Parquet on cloud | {len(results)} rows | {cloud_path}")
                        return True
                    else:
                        self.logger.error(f"- Failed to upload results to {cloud_path}")
                        return False
                        
                finally:
                    # Clean up temp file in the finally block to ensure it happens
                    if tmp_file_path and os.path.exists(tmp_file_path):
                        try:
                            os.remove(tmp_file_path)
                        except Exception as e:
                            self.logger.warning(f"Could not remove temporary file {tmp_file_path}: {e}")
                            # Not a critical error, we can continue
                    
        except Exception as e:
            self.logger.error(f"- Error writing results to Parquet: {e}")
            return False
   
    # Function for extracting cluster details from config
    def get_cluster_detail(self, config, cluster):
        for cluster_detail in config.get("cluster_details", []):
            if cluster in cluster_detail.get("clusters"):
                return cluster_detail
                
        self.logger.warning(f"Cluster {cluster} not found in configuration")
        return None

    # Function for processing a single device
    def process_single_device(
        self, cluster, device_id, cluster_detail, cluster_aggregations, date_path
    ):
        import pandas as pd
        import pyarrow.parquet as pq
        
        device_results = []
        
        try:
            # identify trip windows for the device & day based on the cluster trip message
            trip_message = cluster_detail.get("details", {}).get("trip_identifier", {}).get("message", "")
            if trip_message == "":
                self.logger.info(f"--- No trip identifier message found for cluster {cluster}")
                return []

            # Get trip windows for the device using the trip identifier message
            trip_path = f"{device_id}/{trip_message}/{date_path}"
            trip_windows = self.get_trip_windows(trip_path)
            if len(trip_windows) == 0:
                # self.logger.info(f"--- No trip windows found for {device_id}")
                return []
            
            # extract data aggregation values per trip and add to device_results
            aggregations_list = cluster_aggregations
            for agg in aggregations_list:
                agg_message = agg.get("message", "")
                if not agg_message:
                    continue
                    
                agg_path = f"{device_id}/{agg_message}/{date_path}"
                
                # list files in message directory path
                files = self.list_parquet_files(agg_path)
                if len(files) == 0:
                    self.logger.info(f"--- No Parquet files found for {agg_message} - skipping")
                    continue

                # download files to temp dir and process
                with tempfile.TemporaryDirectory(prefix=f"temp_{device_id}_{agg_message}_{date_path.replace('/', '_')}_") as temp_dir:
                    local_files = self.get_parquet_files(files, temp_dir)
                    if not local_files:
                        self.logger.info(f"--- Failed to download files for {agg_message}")
                        continue
                        
                    # Read parquet files into dataframes and concatenate
                    dfs = [pq.read_table(f).to_pandas() for f in local_files]
                    if not dfs:
                        continue
                        
                    df = pd.concat(dfs, ignore_index=True)
                    
                    # Process each trip window
                    for trip_window in trip_windows:
                        agg_results = self.process_aggregation_for_trip(
                            device_id,
                            agg_message,
                            agg.get("signal", []),
                            agg.get("aggregation", []),
                            trip_window,
                            cluster,
                            df
                        )
                        
                        if agg_results:
                            device_results.extend(agg_results)
                            
        except Exception as e:
            self.logger.error(f"Error processing device {device_id}: {e}")
            return []
            
        return device_results

    # Function for overall processing of the data lake
    def process_data_lake(self):
    
        config = self.load_aggregation_json()
      
        if not config or config == []:
            self.logger.error("No valid configuration to process")
            return 0
            
        days_processed = 0
        total_days = (self.end_date - self.start_date).days + 1
        self.logger.info(f"Processing data from {self.start_date} to {self.end_date}")
        
        # Process each day in range
        for i, single_date in enumerate(self.daterange(), start=1):
            self.logger.info(f"Processing date {i}/{total_days}: {single_date}")
            daily_results = []
            date_path = single_date.strftime("%Y/%m/%d")
            
            # Process each cluster of devices
            for device_cluster in config.get("device_clusters", []):
                cluster = device_cluster.get("cluster")
                cluster_detail = self.get_cluster_detail(config, cluster)
                
                if not cluster_detail:
                    continue
                    
                cluster_aggregations = cluster_detail.get("details", {}).get("aggregations", [])
                self.logger.info(f"- Processing cluster: {cluster}")
                
                # Process each device in cluster
                for device_id in device_cluster.get("devices", []):
                    self.logger.info(f"-- Processing device: {device_id}")
                    
                    device_results = self.process_single_device(
                        cluster,
                        device_id,
                        cluster_detail,
                        cluster_aggregations,
                        date_path
                    )
                    
                    if device_results:
                        daily_results.extend(device_results)
                        
            # Write results for this day if any
            if daily_results:
                days_processed += 1
                self.write_results_to_parquet(daily_results, single_date)
            else:
                self.logger.info(f"- No data extracted for {single_date}")
                
        self.logger.info(f"Stored {days_processed} days with data across {total_days} days")
        return days_processed
