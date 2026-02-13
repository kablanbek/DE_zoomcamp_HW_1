import pandas as pd
from sqlalchemy import create_engine

url_1 = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet"
green_taxi_df = pd.read_parquet(url_1)

url_2 = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"
taxi_zone_df = pd.read_csv(url_2)


def run():
    pg_user = "root"
    pg_pass = "root"
    pg_host = "localhost"
    pg_port = "5432"
    pg_db = "ny_taxi"

    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    green_taxi_df.head(n=0).to_sql(name='green_taxi_data', con=engine, if_exists='replace')
    green_taxi_df.to_sql(name='green_taxi_data', con=engine, if_exists='append')

    taxi_zone_df.head(n=0).to_sql(name='taxi_zone_data', con=engine, if_exists='replace')
    taxi_zone_df.to_sql(name='taxi_zone_data', con=engine, if_exists='append')

if __name__ == '__main__':
    run()



