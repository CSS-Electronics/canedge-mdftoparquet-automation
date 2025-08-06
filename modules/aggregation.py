from datetime import datetime, timedelta
import os
import json
import logging
import pandas as pd
from pathlib import Path
import sys
import tempfile
from modules.cloud_functions import download_object, upload_object, list_objects, normalize_object_path
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
        
    Note: The following parameters are now extracted from the aggregations.json file:
        - trip_gap_min: Gap in minutes to determine a new trip
        - trip_min_length_min: Minimum trip length in minutes
        - date configuration (mode, start_date, end_date)
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
        
        # Default trip parameters (will be overridden by config)
        self.trip_gap_min = 10
        self.trip_min_length_min = 1
        
        # Default date parameters (will be overridden by config)
        self.start_date = datetime.today() - timedelta(days=1)
        self.end_date = self.start_date
            
        # Setup logger if not provided
        if logger is None:
            self.logger = logging.getLogger("AggregateData")
            if not self.logger.handlers:
                handler = logging.StreamHandler(sys.stdout)
                formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
                handler.setFormatter(formatter)
                self.logger.addHandler(handler)
                self.logger.setLevel(logging.INFO)
        else:
            self.logger = logger
            
        self.logger.info(f"Initialized AggregateData with cloud={cloud}, input_bucket={input_bucket}, output_bucket={output_bucket}")

    def load_aggregation_json(self):
        """
        Load aggregations JSON file from cloud storage or local disk
        
        Returns:
            dict: The configuration from the aggregations.json file
        """
        config = None
        
        try:
            if self.cloud == "Local" and os.path.isdir(self.input_bucket):
                # For Local, directly load the file if it exists
                file_path = os.path.join(self.input_bucket, self.aggregations_file)
                if os.path.isfile(file_path):
                    self.logger.info(f"Loading aggregation config from local file: {file_path}")
                    with open(file_path, 'r') as file:
                        config = json.load(file)
                        self.logger.info(f"Data aggregation config loaded successfully")
                else:
                    self.logger.error(f"Aggregation file not found: {file_path}")
                    return None
            else:
                # Create a temporary directory for file operations
                with tempfile.TemporaryDirectory() as temp_dir:
                    # Create download objects handler with temporary directory
                    downloader = DownloadObjects(
                        self.cloud,
                        self.client,
                        self.input_bucket,
                        Path(temp_dir),
                        self.logger
                    )
                    
                    # Use download_json_file to get and parse the JSON in one step
                    self.logger.info(f"Loading aggregation config from {self.input_bucket}/{self.aggregations_file}")
                    config = downloader.download_json_file(self.aggregations_file)
                    
                    if not config:
                        self.logger.error(f"Failed to download or parse aggregations file from {self.input_bucket}")
                        return None
                        
        except Exception as e:
            self.logger.error(f"Error loading JSON file: {e}")
            return None

        if config:
            # Extract clusters count from configuration
            cluster_count = len(config.get('cluster_details', []))
            self.logger.info(f"Data aggregation config loaded with {cluster_count} cluster details")
            
            # Extract date and trip parameters from configuration if they exist
            self._extract_config_parameters(config)
            
            return config
        else:
            self.logger.error("Failed to load aggregation configuration")
            return None
            
    def _extract_config_parameters(self, config):
        """
        Extract date and trip parameters from configuration
        
        Args:
            config (dict): Configuration dictionary
            
        Raises:
            ValueError: If the date configuration is invalid or missing required parameters
        """
        # Extract trip parameters
        if 'config' in config and 'trip' in config['config']:
            trip_config = config['config']['trip']
            
            if 'trip_gap_min' in trip_config:
                self.trip_gap_min = trip_config['trip_gap_min']
                self.logger.info(f"Using trip gap minutes from config: {self.trip_gap_min}")
                
            if 'trip_min_length_min' in trip_config:
                self.trip_min_length_min = trip_config['trip_min_length_min']
                self.logger.info(f"Using trip minimum length from config: {self.trip_min_length_min}")
        
        # Extract date parameters
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
            self.logger.info(f"Using previous day mode: {self.start_date.strftime('%Y-%m-%d')}")
        elif date_mode == 'specific_period':
            # Parse start_date and end_date from config - no defaults allowed
            if 'start_date' not in date_config or 'end_date' not in date_config:
                raise ValueError("For 'specific_period' mode, both 'start_date' and 'end_date' must be specified")
                
            start_date_str = date_config['start_date']
            end_date_str = date_config['end_date']
            
            try:
                self.start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
                self.end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
                self.logger.info(f"Using date range from config: {start_date_str} to {end_date_str}")
            except ValueError as e:
                raise ValueError(f"Invalid date format in configuration. Dates must be in 'YYYY-MM-DD' format: {e}")
        else:
            raise ValueError(f"Unknown date mode: {date_mode}. Supported modes: 'previous_day', 'specific_period'")


    def get_all_device_ids(self):
        """
        Extract all device IDs from the output bucket
        
        Returns:
            list: List of device IDs
        """
        import re
        
        device_ids = []
        
        try:
            # List objects in the output bucket to get all device IDs
            result = list_objects(self.cloud, self.client, self.output_bucket, self.logger, prefix="")
            
            # Extract unique directory names at root level (device IDs)
            paths = [obj["name"] for obj in result.get("objects", [])]
            
            # Find unique device ID prefixes (directories at root level)
            for path in paths:
                parts = path.strip("/").split("/")
                if len(parts) >= 1:
                    potential_id = parts[0]
                    if re.match(r"^[0-9A-F]{8}$", potential_id) and potential_id not in device_ids:
                        device_ids.append(potential_id)
                        
            self.logger.info(f"Found {len(device_ids)} device IDs in output bucket")
            
        except Exception as e:
            self.logger.error(f"Error retrieving device IDs: {e}")
            
        return device_ids

    def list_parquet_files(self, prefix):
        """
        List Parquet files in the output bucket with the given prefix
        
        Args:
            prefix (str): Prefix to filter the files by
            
        Returns:
            list: List of Parquet file paths
        """
        parquet_files = []
        
        try:
            # List objects with the given prefix
            result = list_objects(self.cloud, self.client, self.output_bucket, self.logger, prefix=prefix)
            
            # Filter for Parquet files
            for obj in result.get("objects", []):
                file_path = obj["name"]
                if file_path.endswith(".parquet"):
                    parquet_files.append(file_path)
                    
            self.logger.info(f"Found {len(parquet_files)} Parquet files with prefix {prefix}")
            
        except Exception as e:
            self.logger.error(f"Error listing Parquet files: {e}")
            
        return parquet_files

    def get_parquet_files(self, files, temp_dir):
        """
        Download Parquet files from the output bucket to a temporary directory
        
        Args:
            files (list): List of file paths to download
            temp_dir (str): Temporary directory to store the files
            
        Returns:
            list: List of local file paths
        """
        # Create a DownloadObjects instance for efficient downloading
        downloader = DownloadObjects(
            self.cloud,
            self.client,
            self.output_bucket,
            Path(temp_dir),
            self.logger
        )
        
        local_files = []
        
        for file_path in files:
            local_path = os.path.join(temp_dir, os.path.basename(file_path))
            
            success = downloader.download_file(file_path, Path(local_path))
            
            if success:
                local_files.append(local_path)
            else:
                self.logger.error(f"Failed to download {file_path}")
                
        return local_files

    def get_trip_windows(self, trip_path):
        """
        Identify trip windows based on time gaps in the data
        
        Args:
            trip_path (str): Path to the Parquet file with trip data
            
        Returns:
            list: List of trip windows (start and end timestamps)
        """
        try:
            # Load Parquet data
            df = pd.read_parquet(trip_path)
            
            if 'TimeStamp' not in df.columns:
                self.logger.warning(f"No TimeStamp column in {trip_path}")
                return []
                
            # Sort by TimeStamp
            df = df.sort_values('TimeStamp')
            
            if len(df) == 0:
                return []
                
            # Convert to datetime if not already
            if not pd.api.types.is_datetime64_any_dtype(df['TimeStamp']):
                df['TimeStamp'] = pd.to_datetime(df['TimeStamp'])
                
            # Calculate time diffs
            df['TimeDiff'] = df['TimeStamp'].diff().dt.total_seconds() / 60
            
            # Find gaps larger than trip_gap_min
            trip_breaks = df[df['TimeDiff'] > self.trip_gap_min].index.tolist()
            
            # Initialize trip windows
            trip_windows = []
            
            if len(trip_breaks) == 0:
                # Only one trip
                start_time = df['TimeStamp'].iloc[0]
                end_time = df['TimeStamp'].iloc[-1]
                
                # Check if trip is long enough
                trip_length = (end_time - start_time).total_seconds() / 60
                if trip_length >= self.trip_min_length_min:
                    trip_windows.append((start_time, end_time))
            else:
                # Multiple trips
                start_idx = 0
                
                for break_idx in trip_breaks:
                    # Get trip window
                    start_time = df['TimeStamp'].iloc[start_idx]
                    end_time = df['TimeStamp'].iloc[break_idx - 1]
                    
                    # Check if trip is long enough
                    trip_length = (end_time - start_time).total_seconds() / 60
                    if trip_length >= self.trip_min_length_min:
                        trip_windows.append((start_time, end_time))
                        
                    # Update start index
                    start_idx = break_idx
                    
                # Last trip
                if start_idx < len(df):
                    start_time = df['TimeStamp'].iloc[start_idx]
                    end_time = df['TimeStamp'].iloc[-1]
                    
                    trip_length = (end_time - start_time).total_seconds() / 60
                    if trip_length >= self.trip_min_length_min:
                        trip_windows.append((start_time, end_time))
                
            self.logger.info(f"Found {len(trip_windows)} trip windows in {trip_path}")
            return trip_windows
            
        except Exception as e:
            self.logger.error(f"Error identifying trip windows: {e}")
            return []

    def daterange(self):
        """
        Generate a range of dates to process
        
        Returns:
            generator: Generator yielding dates in the specified range
        """
        for n in range((self.end_date - self.start_date).days + 1):
            yield self.start_date + timedelta(n)

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
        """
        Process aggregation for a single trip window
        
        Args:
            device_id (str): Device ID
            message (str): Message name
            signals (list): List of signals to aggregate
            aggregation_types (list): List of aggregation types
            trip_window (tuple): Start and end timestamps for the trip
            cluster_name (str): Cluster name
            df (DataFrame): DataFrame with data
            
        Returns:
            list: List of aggregation results
        """
        start_time, end_time = trip_window
        
        # Filter DataFrame for the trip window
        trip_df = df[(df['TimeStamp'] >= start_time) & (df['TimeStamp'] <= end_time)]
        
        if len(trip_df) == 0:
            return []
            
        # Format timestamps
        start_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
        end_str = end_time.strftime("%Y-%m-%d %H:%M:%S")
        
        self.logger.info(f"Processing trip from {start_str} to {end_str} for device {device_id}, message {message}")
        
        # Calculate aggregations
        results = []
        
        for signal in signals:
            if signal not in trip_df.columns:
                self.logger.warning(f"Signal {signal} not found in data")
                continue
                
            # Calculate aggregations for this signal
            for agg_type in aggregation_types:
                if agg_type == 'avg':
                    value = trip_df[signal].mean()
                elif agg_type == 'min':
                    value = trip_df[signal].min()
                elif agg_type == 'max':
                    value = trip_df[signal].max()
                elif agg_type == 'count':
                    value = len(trip_df)
                else:
                    self.logger.warning(f"Unsupported aggregation type: {agg_type}")
                    continue
                    
                # Create result record
                result = {
                    'device_id': device_id,
                    'cluster': cluster_name,
                    'message': message,
                    'signal': signal,
                    'aggregation': agg_type,
                    'value': float(value),
                    'start_time': start_str,
                    'end_time': end_str,
                    'date': start_time.strftime("%Y-%m-%d")
                }
                
                results.append(result)
                
        return results

    def write_results_to_parquet(self, results, date):
        """
        Write aggregation results to a Parquet file
        
        Args:
            results (list): List of aggregation results
            date (datetime): Date for the file name
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not results:
            self.logger.info("No results to write")
            return False
            
        try:
            # Convert results to DataFrame
            df = pd.DataFrame(results)
            
            # Convert datetime columns to pandas datetime objects
            for col in ["start_time", "end_time"]:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col])
                    
            # Format date for file name
            date_str = date.strftime("%Y-%m-%d")
            
            # Use temporary file with context manager
            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
                temp_file_path = temp_file.name
                
                # Write to temporary file (closing it first since pandas will reopen it)
                temp_file.close()
                df.to_parquet(temp_file_path, index=False)
                
                # Define output path
                output_path = f"{self.aggregations_folder}/{self.table_name}/{date_str}.parquet"
                
                # Upload to output bucket
                success = upload_object(
                    self.cloud,
                    self.client,
                    self.output_bucket,
                    output_path,
                    temp_file_path,
                    self.logger
                )
                
                # Clean up temp file
                os.remove(temp_file_path)
                
                if success:
                    self.logger.info(f"Successfully wrote {len(results)} results to {output_path}")
                    return True
                else:
                    self.logger.error(f"Failed to upload results to {output_path}")
                    return False
                    
        except Exception as e:
            self.logger.error(f"Error writing results to Parquet: {e}")
            return False

    def get_cluster_detail(self, config, cluster):
        """
        Get cluster details from configuration
        
        Args:
            config (dict): Configuration dictionary
            cluster (str): Cluster name
            
        Returns:
            dict: Cluster details or None if not found
        """
        if not config:
            return None
            
        # Find cluster in cluster_details
        for cluster_detail in config.get("cluster_details", []):
            if cluster_detail.get("cluster") == cluster:
                return cluster_detail
                
        self.logger.warning(f"Cluster {cluster} not found in configuration")
        return None

    def process_single_device(
        self, cluster, device_id, cluster_detail, cluster_aggregations, date_path
    ):
        """
        Process a single device for the specified date
        
        Args:
            cluster (str): Cluster name
            device_id (str): Device ID
            cluster_detail (dict): Cluster details
            cluster_aggregations (list): List of aggregation configurations
            date_path (str): Path to the date directory (YYYY/MM/DD)
            
        Returns:
            list: List of aggregation results
        """
        device_results = []
        
        try:
            # List Parquet files for this device/date
            prefix = f"{device_id}/{date_path}"
            files = self.list_parquet_files(prefix)
            
            if not files:
                self.logger.info(f"No files found for {device_id} on {date_path}")
                return []
            
            # Use temporary directory with context manager for clean auto-removal
            with tempfile.TemporaryDirectory(prefix=f"temp_{device_id}_{date_path.replace('/', '_')}_") as temp_dir:
                # Download files
                local_files = self.get_parquet_files(files, temp_dir)
                
                if not local_files:
                    return []
                    
                # Process each aggregation from cluster config
                for aggregation in cluster_aggregations:
                    message = aggregation.get('message')
                    signals = aggregation.get('signals', [])
                    aggregation_types = aggregation.get('aggregation_types', ['avg', 'min', 'max', 'count'])
                    
                    self.logger.info(f"Processing {message} with {len(signals)} signals")
                    
                    # Find files matching this message
                    message_files = [f for f in local_files if message.lower() in f.lower()]
                    
                    for trip_path in message_files:
                        try:
                            # Read data
                            df = pd.read_parquet(trip_path)
                            
                            # Get trip windows
                            trip_windows = self.get_trip_windows(trip_path)
                            
                            # Process each trip
                            for trip_window in trip_windows:
                                agg_results = self.process_aggregation_for_trip(
                                    device_id, message, signals, aggregation_types, trip_window, cluster, df
                                )
                                device_results.extend(agg_results)
                                
                        except Exception as e:
                            self.logger.error(f"Error processing {trip_path}: {e}")
            
            # Temporary directory is automatically cleaned up after the with block
            return device_results
            
        except Exception as e:
            self.logger.error(f"Error processing device {device_id}: {e}")
            return []

    def process_data_lake(self, config=None):
        """
        Process the data lake for all configured devices
        
        Args:
            config (dict): Configuration dictionary or None to load from file
            
        Returns:
            int: Number of days with data processed
        """
        if not config:
            config = self.load_aggregation_json()
            
        if not config:
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
                    
                cluster_aggregations = cluster_detail.get(self.aggregations_folder, [])
                self.logger.info(f"Processing cluster: {cluster}")
                
                # Process each device in cluster
                for device_id in device_cluster.get("devices", []):
                    self.logger.info(f"Processing device: {device_id}")
                    
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
                self.logger.info(f"No data extracted for {single_date}")
                
        self.logger.info(f"Stored {days_processed} days with data across {total_days} days")
        return days_processed
