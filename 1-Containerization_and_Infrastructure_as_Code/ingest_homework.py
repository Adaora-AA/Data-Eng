#!/usr/bin/env python
# coding: utf-8

import os
import argparse
import requests
from time import time
import pandas as pd
from sqlalchemy import create_engine

def download_file(url, output_file):
    "Download file from the URL and save it locally."
    if os.path.exists(output_file):
        print(f"{output_file} already exists. Skipping download.")
        return
    try:
        print(f"Downloading {url} to {output_file}...")
        os.system(f"wget {url} -O {output_file}")
        print(f"Downloaded {output_file} successfully.")
    except requests.exceptions.RequestException as e:
        print(f"Failed to download {url}. Error: {e}")
        raise



def ingest_taxi_data(csv_file, table_name, engine):
    "Ingest taxi trip data in chunks to the database."
    print(f"Ingesting taxi data from {csv_file} into table {table_name}...")
    try:
        df_iter = pd.read_csv(csv_file, iterator=True, chunksize=100000)
        df = next(df_iter)
        df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
        df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
        df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')
        df.to_sql(name=table_name, con=engine, if_exists='append')
        
        while True:
            t_start = time()
            try:
                df = next(df_iter)
                df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
                df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
                df.to_sql(name=table_name, con=engine, if_exists='append')
                t_end = time()
                print(f"Inserted another chunk, took {t_end - t_start:.3f} seconds")
            except StopIteration:
                print(f"Finished ingesting {csv_file} into the {table_name} table.")
                break
    except Exception as e:
        print(f"Failed to ingest taxi data. Error: {e}")
        raise

def ingest_zones_data(csv_file, table_name, engine):
    "Ingest taxi zone data to the database."
    print(f"Ingesting zones data from {csv_file} into table {table_name}...")
    try:
        df = pd.read_csv(csv_file)
        df.to_sql(name=table_name, con=engine, if_exists='replace')
        print(f"Finished ingesting zones data into the {table_name} table.")
    except Exception as e:
        print(f"Failed to ingest zones data. Error: {e}")


def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name1 = params.table_name1
    table_name2 = params.table_name2
    url1 = params.url1
    url2 = params.url2
    
    # Determine the correct extension
    try:
        # Define the file paths
        csv_file = 'output.csv'
        csv_gz_file = 'output.csv.gz'
        parquet_file = 'output.parquet'

        # Determine the file type based on the URL and process accordingly
        if url1.endswith('.csv.gz'):
            download_file(url1, csv_gz_file)
            csv1 = csv_gz_file
        elif url1.endswith('.csv'):
            download_file(url1, csv_file)
            csv1 = csv_file
        elif url1.endswith('.parquet'):
            download_file(url1, parquet_file)
            # Convert the Parquet file to CSV
            df = pd.read_parquet(parquet_file)
            df.to_csv(csv_gz_file, index=False, compression='gzip')  # Save as  gzipped CSV
            csv1 = csv_gz_file
            print(f"Converted {parquet_file} to {csv_gz_file} for ingestion.")
        else:
            raise ValueError("Unsupported file type. URL must end with '.csv', '.csv.gz', or '.parquet'.")
        
    except Exception as e:
        print(f"Error determining file type or processing file: {e}")

    csv2 = 'output2.csv.gz' if url2.endswith('.csv.gz') else 'output2.csv'
    
    # Download the the taxi zone files
    download_file(url2, csv2)

    # Create database engine
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    
    # Ingest data
    ingest_taxi_data(csv1, table_name1, engine)
    ingest_zones_data(csv2, table_name2, engine)

    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name1', required=True, help='name of the yellow_taxi_trip table')
    parser.add_argument('--table_name2', required=True, help='name of the zones table')
    parser.add_argument('--url1', required=True, help='url of the first csv file')
    parser.add_argument('--url2', required=True, help='url of the second csv file')

    args = parser.parse_args()
    # print("Ingesting data from:", args.url)

    main(args)