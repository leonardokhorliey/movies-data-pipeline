from pathlib import Path
import pandas as pd
import os
from sqlalchemy import create_engine
from datetime import datetime
from time import time
from prefect import flow, task, Flow
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket


@task(log_prints=True)
def fetch(dataset_url, file_name, output_path) -> pd.DataFrame:
    """Read Data from web to DataFrame"""
    os.system(f"~/Desktop/CodeWorks/DataEngineering/zoomcamp-proj/zoomcamp/bin/kaggle datasets download {dataset_url} -f {file_name} -p {output_path}")
    print("Pulled")
    df = pd.read_csv(f"prefect-flow/data/{file_name}.zip")
    return df


@task(log_prints=True)
def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Separate text field into a separate dataframe"""


    df_without_text = df[['message_id', 'parent_id', 'user_id', 'created_date', 'role', 'lang', 'review_count', 'review_result', 'deleted']]
    print(df_without_text.dtypes)
    df_without_text['created_date'] = pd.to_datetime(df_without_text['created_date'])
    print(df_without_text.dtypes)
    text_df = df[['message_id', 'text']]

    return [df_without_text, text_df]

@task()
def write_local(df: pd.DataFrame, dataset_file: str) -> Path:
    path = Path(f"prefect-flow/data/{dataset_file}.parquet")
    df.to_parquet(path, compression='gzip')

    return path

@task()
def write_to_gcs(path: Path) -> None:
    gcp_cloud_storage_bucket_block = GcsBucket.load("prefect-gcs")
    gcp_cloud_storage_bucket_block.upload_from_path(
        from_path=path,
        to_path=f"data/{path}"
    )




def etl_web_to_gcs() -> None:
    """The main ETL function"""
    dataset_file = "oasst1-val.csv"
    dataset_url = "snehilsanyal/oasst1"
    output_path = "~/Desktop/CodeWorks/DataEngineering/zoomcamp-proj/prefect-flow/data/"
    print(dataset_url)
    df = fetch(dataset_url, dataset_file, output_path)
    separated_dfs = clean_data(df)
    check = 1
    for df in separated_dfs:

        path = write_local(df, dataset_file.replace(".csv", "") + ("_no_text" if check == 1 else "_text"))

        # write_to_gcs(path)
        check += 1

fl= Flow(etl_web_to_gcs, name="GCS Flow")



if __name__ == '__main__':
    fl._run()