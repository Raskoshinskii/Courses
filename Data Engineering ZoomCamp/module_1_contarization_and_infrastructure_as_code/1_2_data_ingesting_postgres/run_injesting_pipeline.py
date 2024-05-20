import pandas as pd
import os
import argparse
import pyarrow.parquet as pq

from sqlalchemy import create_engine
import time


def measure_time(func):
    """Measure execution time."""
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)  # call the original function
        end_time = time.time() 
        print(f"Time took: {end_time - start_time:.2f} seconds")
        return result
    return wrapper


@measure_time
def parquet_to_csv(f_name_pq: str, f_name_csv: str) -> None:
    """Transform parquet to csv."""
    print('Start parquet conversion')
    df = pq.read_table(f_name_pq).to_pandas()
    df.to_csv(f_name_csv, index=False)
    print('Successfully converted to .csv')


@measure_time
def download_taxi_data(url: str) -> None:
    """Downloads dataset from URL."""
    try:
        f_name = url.split('/')[-1]
        os.system(f'wget {url} -O data/{f_name}')
        print(f'Successfully Downloaded: {f_name}')
    except:
        print('Downloading Error. Check parameters!')


def get_arguments():
    """Generate arguments neededf for the Pipeline."""
    parser = argparse.ArgumentParser(description='Ingesting CSV dataset into Postgres')

    parser.add_argument('--user', help='user name for Postgres DB')
    parser.add_argument('--password', help='password for Postgres DB')
    parser.add_argument('--host', help='host for Postgres DB')
    parser.add_argument('--port', help='port for Postgres DB')
    parser.add_argument('--db_name', help='data base name for Postgres DB')
    parser.add_argument('--pg_table_name', help='table name for Postgres DB')
    parser.add_argument('--url', help='url of the dataset')
    parser.add_argument('--n_rows_read', type=int, help='how many rows to read from a dataset')
    parser.add_argument('--chunksize', type=int, help='n rows per chunk/batch')

    args = parser.parse_args()
    return args


def run_pipeline(params) -> None:
    """
    Start Postgres data ingestion pipeline using batches.
    
    """    
    # create engine for postgres db and get schema
    engine = create_engine(f"postgresql://{params.user}:{params.password}@{params.host}:{params.port}/{params.db_name}")

    # read the dataset in batches
    f_name_csv = 'data/' + params.url.split('/')[-1].replace('.parquet', '.csv')
    df_iterator = pd.read_csv(f_name_csv, iterator=True, chunksize=params.chunksize, nrows=params.n_rows_read)

    # create empty table in Postgres
    data = pd.read_csv(f_name_csv, nrows=5)
    data.head(0).to_sql(name=params.pg_table_name, con=engine, if_exists="replace") # if the table exists, it drops it with each new run

    # batch data injestion
    process_data = True
    batch_id = 1
    while process_data:
        try:
            start = time.time()
            df_curent_batch = next(df_iterator)

            # transform datatime columns from object into datetime
            df_curent_batch["tpep_pickup_datetime"] = pd.to_datetime(
                df_curent_batch["tpep_pickup_datetime"]
            )
            df_curent_batch["tpep_dropoff_datetime"] = pd.to_datetime(
                df_curent_batch["tpep_dropoff_datetime"]
            )

            # injest Postgres data
            df_curent_batch.to_sql(name=params.pg_table_name, con=engine, if_exists="append")

            end = time.time()
            elapsed_time = round(end - start, 3)

            print(f"Batch {batch_id} Successfully Injested! Time took: {elapsed_time} seconds")
            batch_id += 1
        except Exception as e:
            process_data = False
            print(e)
            print("There is no further batch. End of Ingesting")


if __name__ == '__main__':
    args = get_arguments()
    f_name_pq = 'data/' + args.url.split('/')[-1]
    f_name_csv = f_name_pq.replace('.parquet', '.csv')

    if not os.path.exists(f_name_csv):
        if not os.path.exists(f_name_pq):
            download_taxi_data(args.url)
        else:
            parquet_to_csv(f_name_pq, f_name_csv)
    else:
        print(f'{f_name_csv} already exists!')

    run_pipeline(args)
    print(f'Successfully injested {args.n_rows_read} rows to {args.pg_table_name} table')
