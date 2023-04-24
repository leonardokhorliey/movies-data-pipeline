from pathlib import Path
import pandas as pd
import os
from sqlalchemy import create_engine
from datetime import datetime
from time import time
from prefect import flow, task, Flow
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket

def set_month_column(timestamp):
    date = datetime.fromtimestamp(timestamp)
    return date.year *100 + date.month

@task(log_prints=True)
def fetch(dataset_url, output_path) -> pd.DataFrame:
    """Read Data from web to DataFrame"""
    try:
        file = open(output_path, 'r')
    except FileNotFoundError:
        os.system(f"wget {dataset_url} -O {output_path}")
        os.system(f"tar xzvf {output_path}")
        print("Pulled")


@task(log_prints=True)
def read_df(file_name):
    """Read CSV from local path"""
    df = pd.read_csv(f"ml-25m/{file_name}.csv")

    return df


@task(name='movie_from_genre_separator', log_prints=True)
def clean_movies_df(movies_df):
    """Separates the genre data as a standalone dataframe from that of the full movies Dataframe"""
    movie_to_genre_df = movies_df[['movieId', 'genres']]
    movie_to_genre_df['genres'] = movie_to_genre_df['genres'].str.split('|')

    movie_to_genre = movie_to_genre_df.explode('genres').reset_index(drop=True)

    return [movies_df[['movieId', 'title']], movie_to_genre]



@task(name='writing_to_local', log_prints=True)
def write_to_local(df: pd.DataFrame, file_name):
    """Writes Dataframe to local storage as parquet, to then export to Google Cloud storage.
        Splits the very large datasets (`ratings` and `genome-scores`) into partitioned chunks, based on month and row indices.
    """
    path = Path(f"data/pq/{file_name}")
    os.system("mkdir data/pq")
    file_names = []
    if file_name in ['ratings']:
        os.system(f"mkdir {path}")

        df['year_month'] = df['timestamp'].apply(set_month_column)
        max_month_float = df['year_month'].max()
        min_month_float = 201601

        while min_month_float <= max_month_float:
            
            file_names.append(str(min_month_float))
            df_for_month = df[df['year_month'] == min_month_float]
            df_for_month.to_parquet(f'{path}/{min_month_float}.parquet', compression='gzip')

            # print(min_month_float)
            if (min_month_float - 12) % 100 == 0:
                min_month_float = (min_month_float - 12) + 100
            
            min_month_float += 1

        return {
            "path": path,
            "files": file_names
        }

    elif file_name in ['genome-scores']:
        os.system(f"mkdir {path}")
        df_len = df.shape[0]
        counter = 1
        for i in range(0, df_len, 50000):
            df_split = df.iloc[i: i+50000]

            file_names.append(f'file_{counter}')
            df_split.to_parquet(f'{path}/file_{counter}.parquet', compression='gzip')
            counter += 1

        return {
            "path": path,
            "files": file_names
        }

    else:
        df.to_parquet(f'{path}.parquet', compression='gzip')
        return {
            "path": path,
            "files": None
        }
    

@task(name='writing_to_gcs', log_prints=True)
def write_to_gcs(path_dict: dict):
    """Writes file to Google Cloud storage based on the path dictionary returned from `write_to_local`"""
    gcp_cloud_storage_bucket_block = GcsBucket.load("prefect-gcs")
    
    pth = path_dict["path"]
    if path_dict["files"] == None:
        local_path = f"{pth}.parquet"
        gcp_cloud_storage_bucket_block.upload_from_path(
            from_path=local_path,
            to_path=f"zoomcamp-project-data/{local_path}"
        )

    else:
        for file in path_dict["files"]:
            local_path = f"{pth}/{file}.parquet"
            gcp_cloud_storage_bucket_block.upload_from_path(
                from_path=local_path,
                to_path=f"zoomcamp-project-data/{local_path}"
            )

    return True


@flow(name='read_movies_data')
def do_transform(dataset_url, save_path):
    """Main Flow Function that handles the ETL job for all the datasets"""
    dataset_url = 'https://files.grouplens.org/datasets/movielens/ml-25m.zip'
    save_path = 'data/dataset'
    expected_data_files = ['movies', 'ratings', 'genome-scores', 'genome-tags']

    fetch(dataset_url, f"{save_path}.zip")

    for data_file in expected_data_files:
        data = read_df(data_file)

        if data_file == 'movies':
            split_dfs = clean_movies_df(data)
            ind = 1
            for df in split_dfs:
                path_dict = write_to_local(df, 'movies' if ind == 1 else 'genres')
                uploaded_to_gcs = write_to_gcs(path_dict)
                ind += 1

        else:
            path_dict = write_to_local(data, data_file)
            uploaded_to_gcs = write_to_gcs(path_dict)

    return uploaded_to_gcs

if __name__ == '__main__':
    argParser = argparse.ArgumentParser()
    argParser.add_argument("-d", "--url", type= str, help="URL with file")
    argParser.add_argument("-s", "--path", type= str, help="Path to save the file to")

    args = argParser.parse_args()

    do_transform(args.url, args.path)




        




    


