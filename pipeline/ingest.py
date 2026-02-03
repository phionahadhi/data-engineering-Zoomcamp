#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click


dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]


def run(
    pg_user: str, 
    pg_pass: str,
    pg_host: str,
    pg_port: int,
    pg_db: str,
    year: int,
    month: int,
    target_table:str,
    chunk_size: int,
):
    """Ingest CSV into Postgres — parameters are passed in (CLI-enabled)."""

    #data source
    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
    url = f'{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz'

    #creates connection pool to postgres database
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')


    #chunked csv reading
    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunk_size
    )
    first = True

    for df_chunk in tqdm(df_iter):
        if first:
            df_chunk.head(n=0).to_sql(      # pandas creates table with appropriate schema without any data
                name= target_table,
                con=engine,
                if_exists='replace'
            )
            first = False

        df_chunk.to_sql(  #load data to the table in chunks
            name=target_table, 
            con=engine, 
            if_exists='append'
        )

@click.command()
@click.option("--pg-user", default="root", show_default=True, help="Postgres user")
@click.option("--pg-pass", default="root", show_default=True, help="Postgres password")
@click.option("--pg-host", default="localhost", show_default=True, help="Postgres host")
@click.option("--pg-port", default=5432, show_default=True, type=int, help="Postgres port")
@click.option("--pg-db", default="ny_taxi", show_default=True, help="Postgres database")
@click.option("--year", default=2021, show_default=True, type=int, help="Year of data")
@click.option("--month", default=1, show_default=True, type=int, help="Month of data (1-12)")
@click.option("--target-table", default="yellow_taxi_data", show_default=True, help="Destination table")
@click.option("--chunk-size", default=100000, show_default=True, type=int, help="Chunk size for pandas.read_csv")
def main(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, target_table, chunk_size):
    run(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, target_table, chunk_size)

if __name__ == '__main__':
    main()
