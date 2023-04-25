# Movies Data Pipeline

Final Project for [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp)


## Project Description
-------------------------
Movies Data Pipeline is a simple Data pipeline set up to replicate an end-to-end ELT data engineering model use case on the 25 million rows dataset from Movie Lens. It involved the following steps:

1. Extract from source URL
2. Load into a Data Lake with a light transformation
3. Read from Data Lake and carry out further transformations
4. Load into Data Warehouse
5. Visualize Data


## Tools
---------------
* Python 3.11.2

* [Prefect](https://github.com/PrefectHQ)

* [PySpark](https://pypi.org/project/pyspark/)

* [Google Cloud Platform](https://console.cloud.google.com/)

* [Looker Studio](https://lookerstudio.google.com/)

## Dataset Used
----------------------
[Movie Lens Dataset](https://grouplens.org/datasets/movielens/)


## Requirements

* Google Cloud Virtual Machine Instance
* Local IDE (Visual Studio Code preferably)
* Google Cloud Dataproc instance
* Visualization Tool (Metabase, Looker, ...)


## Get Started

### Running Prefect ETL to Data Lake
-----------------------
Set up a Compute Instance (Linux VM preferably) on Google Cloud Platform and SSH into the VM following the directions [here](https://docs.bitnami.com/google/faq/get-started/connect-ssh/#:~:text=Log%20in%20to%20the%20Google,the%20%E2%80%9CSSH%20Keys%E2%80%9D%20field.t). Then, clone the repo with the following command

```git clone https://github.com/leonardokhorliey/movies-data-pipeline && cd movies-data-pipeline
```

Create and activate a Python virtual environment using 

`python -m venv [name_of_env] && source/bin/activate`

and then install the required Prefect packages using 

`pip install -r requirements.txt`

In a new terminal, run `prefect server start` and follow the prompt. You would need to forward the designated port to your local computer to access the Prefect Dashboard. Check [here](https://code.visualstudio.com/docs/remote/ssh) for Visual Studio Code. Detailed documentation on getting fully set with Prefect on GCP can be found [here](https://prefecthq.github.io/prefect-gcp/).

Execute the `prefect_web_to_gcs.py` using the python command with the help command to see required arguments, and then execute normally.


### Running Spark Job to Data Warehouse
--------------------------
Set up a Dataproc cluster on Google Cloud Platform. Also, create a new BigQuery dataset as directed [here](https://cloud.google.com/bigquery/docs/datasets#console). Upload the file `pyspark_gcs_to_bq.py` to Google Cloud Storage, and keep the path. Execute the Spark job by running the following command in the terminal.

```
gcloud dataproc jobs submit pyspark \
    --cluster=[DATAPROC_CLUSTER_NAME] \
    --region=[CLUSTER_REGION] \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    [PATH_TO_UPLOADED_PYTHON_FILE] \
    -- \
       --path=[PATH_TO_YOUR_PARQUET_FILES_IN_DATA_LAKE] \
        --dataset=[BIGQUERY_DATASET] \
        --bucket=[BUCKET_NAME_TO_SAVE_TEMP_ETL_FILES]
```

NB: It is very important to maintain the same region across the resources being used in a particular project. Many times, data transfer is not supported across multiple regions.


### Preparing Visualization
---------------------------

Pick any visualization tool, and follow the docs in whichever case to connect to the BigQuery Dataset and build reports.


More tips can be found directly on the [DE Zoomcamp Repo](https://github.com/DataTalksClub/data-engineering-zoomcamp)



## Visualisation Dashboard

[Movie Lens Dashboard](https://lookerstudio.google.com/reporting/18660276-6a62-4e2b-b35e-f819ba2a7402)

![dashboard](/images/Looker_Studio_Dashboard.png)













