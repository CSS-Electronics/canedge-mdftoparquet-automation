# Parquet data lake data aggregation
This script serves to enable simple data aggregations from a Parquet data lake, stored either locally or on AWS S3.

The script can be deployed locally for a locally stored data lake, or for processing an AWS S3 based data lake.

The script is primarily designed for deployment in AWS Glue as a daily scheduled task. See the CANedge Intro Parquet data lake 'advanced topics' for details on deploying this. 

------------------
## Aggregations config file
The script relies on a JSON file, `aggregations.json` which you must update to specify your preferred aggregation settings. If your data lake is stored locally in a folder, the `aggregations.json` should be stored in the folder root. If you are working with an AWS S3 data lake, the `aggregations.json` file should be stored in the root of the S3 bucket containing the data lake. 

The structure of the `aggregations.json` file is illustrated by below example:

```
{
  "device_clusters": [
    {
      "devices": [
        "2A896980",
        "2F6913DB"
      ],
      "cluster": "cluster1"
    },
    {
      "devices": [
        "B21A8F12"
      ],
      "cluster": "cluster2"
    }
  ],
  "cluster_details": [
    {
	  "clusters": ["cluster1","cluster2"],
      "details": {
        "trip_identifier": {
          "message": "CAN2_MessageName"
        },
        "aggregations": [
          {
            "message": "CAN1_MessageName",
            "signal": [
              "SignalName1"
            ],
            "aggregation": ["avg"]
          }
        ]
      }
    }
  ]
}
```

Here, `device_clusters` is a list of cluster definitions, each containing a cluster name (e.g. `cluster1`) and a list of devices related to the cluster (e.g. `["2A896980","2F6913DB"]`). You can use clusters to group devices by either 1) a business-related logic (e.g. to represent different projects, regions, clients etc.) or 2) a DBC-related logic (e.g. if you have different signals you wish to aggregate for different groups of devices). If the `devices` list is left empty (i.e. `"devices": []`), the script will list all device IDs in the folder/bucket. This can help make your script more dynamic if you are frequently adding new devices.

The `cluster_details` is a list of cluster aggregation details. Each entry requires an explicit `trip_identifier` message, which serves the role of identifying trip windows in a given day. The `aggregations` section specifies which message/signals to aggregate at the trip level. For each message you can extract a list of signals, each which will be aggregated based on the list of aggregations.

The default script supports the following aggregation methods: `min`, `max`, `avg`, `median`, `sum`, `first`, `last`, `delta_sum`, `delta_sum_pos`, `delta_sum_neg`. The delta sums are calculated by summing the delta signal values, optionally filtering for only positive or negative deltas. You can easily expand the list by modifying the script accordingly. 

### Regarding message and signal names 
Message and signal names are **CASE SENSITIVE** and must be entered correctly. 

We recommend copy/pasting the message names directly from the relevant folders of your data lake as they must be named identically to the folders (do not copy them from the DBC file, for example) - and to copy paste the signal names by copying them from a Parquet file within the relevant message folder (you can use the free Tad Parquet file viewer for this). Alternatively, you can copy the signal names from your DBC file.

------------------
## Deployment

### Deploy in AWS Glue + process S3 data lake [RECOMMENDED]
The default expected deployment is within AWS Glue and the script variables are by default set up for this. Note that in Glue the script will automatically have access to your S3 output bucket if you have deployed Glue/Athena with the `glue-athena-v2.0.5.json` stack (or newer). 

### Deploy locally + process S3 data lake
To run the script locally to process data from your S3 data lake, you must set `s3 = True` in the script and specify your S3 output bucket in the `bucket_name` variable. 

Further, you will need to add your S3 credentials as environmental variables. For this purpose, you can use the same AWS S3 credentials that you would use when deploying the MinIO client automation for processing a backlog of Parquet files (see the CANedge Intro section on Parquet data lakes). You open a command line and run the following (on Windows):

```
export AWS_ACCESS_KEY_ID='your_access_key_id_here'
export AWS_SECRET_ACCESS_KEY='your_secret_access_key_here'
```


### Deploy locally + process local data lake
You can process a locally stored data lake by placing the script next to the folder containing your data lake. You must specify the folder name in the script by updating the `bucket_name` variable. Further, you must set `s3 = False`.


------------------
## Script customization

## Time period 
The script will by default be set up to process the previous full day. However, you can modify this easily by e.g. setting your own `start_date` and `end_date` in the script. This is useful if you e.g. wish to process all of your historical data to get started. 

## Device color generator
In the folder `device-color-generator/` we provide a basic script that can be used in generating unique, consistent colors for each device explicitly added in the `aggregations.json` file. 

## Performance and S3 transfer costs 
If you are processing a data lake stored in an AWS S3 bucket, you may incur costs when doing so. The primary cost will be data transfer out of the S3 bucket to local disk (if the script is run locally). This cost can be avoided by running the script in AWS Glue in the same region as your S3 bucket. In addition, you will incur costs related to S3 API executions for listing, getting and putting objects. These can be significant if you have a very large number of files in your data lake.

Note also that if you run the script in AWS Glue, you will incur costs for running the Glue script. You pay per minute as per the [Glue pricing](https://aws.amazon.com/glue/pricing/) with the default cost of 0.44$/DPU hour as of 27/03/2024. As an example, we processed a data lake consisting of 1 device with 0.250 GB of Parquet files split across ~10,000 Parquet files and ~330 daily partitions. This took ~15 min in Glue with 2 DPUs, corresponding to a cost of ~0.22$. 

The script is designed as a starting point and may be modified as you see fit, e.g. to extend functionality or improve performance. Note that we do not offer any support on modified scripts. 

You can update your deployed AWS Glue job with a new script via the Script tab and pressing save. 

