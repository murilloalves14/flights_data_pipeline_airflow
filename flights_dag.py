# airflow imports
from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook


# other imports
import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
import s3fs
import io
import requests
import json
import os
import csv
import boto3


def read_csv_from_s3():
    '''This function connects with aws services and get specified file from s3'''
    s3_hook = S3Hook(aws_conn_id='s3_airflow_conn')
    bucket_name = 'fligths-project-datalake'
    file_key = 'airports.csv'
    csv_data = s3_hook.read_key(bucket_name=bucket_name, key=file_key)

    df = pd.read_csv(io.StringIO(csv_data))

    return df


def transform_airports(task_instance):
    '''This function pull data from read_airports_s3 task and do some simple transformations'''

    # getting csv from previous task
    df = task_instance.xcom_pull(task_ids='group_a.read_airports_s3')

    df = df.drop_duplicates()
    df = df.drop(columns=['continent', 'home_link',
                 'wikipedia_link', 'keywords', 'id'])
    df = df[(df['type'] != 'heliport') & (~df['iata_code'].isna())]
    df['scheduled_service'] = df['scheduled_service'].apply(
        lambda x: True if x == 'yes' else False)
    df['elevation_m'] = df['elevation_ft'].apply(lambda x: x * 0.3048)
    df_final = df.reindex(columns=['ident', 'name', 'iata_code', 'gps_code', 'type', 'iso_country',
                                   'iso_region', 'municipality', 'local_code', 'latitude_deg', 'longitude_deg', 'elevation_ft', 'elevation_m', 'scheduled_service'])
    df = df.rename(columns={'ident': 'id', 'iata_code': 'iata', 'municipality': 'city',
                   'latitude_deg': 'latitude', 'longitude_deg': 'longitude'})

    return df


# function to load data into postgres
def load_to_postgres(task_instance, taskId, table_name):
    '''This function loads a dataframe into a table in postgres'''

    df = task_instance.xcom_pull(task_ids=taskId)

    pg_hook = PostgresHook(postgres_conn_id='rds_de_postgres')

    pg_hook.insert_rows(table=table_name, rows=df.values.tolist(),
                        target_fields=df.columns.tolist())


def extract_flight_data_api():
    '''This function extracts data from Flight Data API for the given coordinates'''

    url = "https://flight-data4.p.rapidapi.com/get_area_flights"
    querystring = {"latitude": "-23.4262", "longitude": "-46.48"}

    headers = {
        "X-RapidAPI-Key": "XXXXX",
        "X-RapidAPI-Host": "flight-data4.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)
    results = response.json()

    data = results['flights']

    df = pd.DataFrame(data)
    df = df.to_json()

    return df


def transform_load_flights_pg(task_instance):
    'this function transform the flights table'

    flights = task_instance.xcom_pull(
        task_ids='group_a.extract_api_flight_data')

    df = pd.read_json(flights, orient='records')

    df = df.drop(columns=['arrival_position',
                 'departure_position', 'heading', 'source'])
    df = df.replace("", np.nan)
    df = df.dropna(subset=['arrival', 'flight'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df = df.reindex(columns=['flight', 'airline', 'departure', 'arrival', 'altitude', 'timestamp',
                    'groundspeed', 'verticalspeed', 'registration', 'station', 'type', 'latitude', 'longitude'])
    df = df.rename(columns={'timestamp': 'tracked_time'})

    pg_hook = PostgresHook(postgres_conn_id='rds_de_postgres')

    pg_hook.insert_rows(table='flights', rows=df.values.tolist(),
                        target_fields=df.columns.tolist())


# function to load data from postgres to s3 bucket
def joined_s3(task_instance):
    'This function load the joined data from postgres to s3'
    data = task_instance.xcom_pull(task_ids='join_data')

    df = pd.DataFrame(data, columns=['flight_id', 'airline_id', 'airline_name', 'dep', 'dep_airport', 'dep_country', 'dep_city',
                                     'arr', 'arr_airport', 'arr_country', 'arr_city', 'aircraft', 'altitude', 'day_flight'])

    now = datetime.now()
    dt_string = now.strftime('%Y%m%d%H%M')
    dt_string = 'flights' + dt_string

    df.to_csv(f'//s3/XXXXX/{dt_string}.csv', index=False)


# Airflow Data Pipeline

# defining dag arguments
default_args = {
    'owner': 'murillo',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 25),
    'email': ['murillo.alves14@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': '1',
    'retry_delay': timedelta(minutes=1)
}

# instantiating DAG

with DAG(
    'flights_sp_dag',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False
) as dag:

    # iniciating the pipeline
    start_pipeline = DummyOperator(
        task_id='start_pipeline'
    )

    # joining info from the 3 tables created
    join_data = PostgresOperator(
        task_id='join_data',
        postgres_conn_id='rds_de_postgres',
        sql='''SELECT
            f.flight AS flight_id,
            f.airline AS airline_id,
            al.name AS airline_name,
            f.departure AS dep,
            ap1.name AS dep_airport,
            ap1.iso_country AS dep_country,
            ap1.city AS dep_city,
            f.arrival AS arr,
            ap2.name AS arr_airport,
            ap2.iso_country AS arr_country,
            ap2.city AS arr_city,
            f.type AS aircraft,
            f.altitude,
            f.tracked_time AS day_flight
        FROM
            (SELECT DISTINCT * FROM flights) AS f
        JOIN airlines al ON f.airline = al.ICAO
        JOIN airports ap1 ON f.departure = ap1.iata
        JOIN airports ap2 ON f.arrival = ap2.iata;
        
        '''
    )

    # loading data into s3
    load_joined_to_s3 = PythonOperator(
        task_id='load_joined_to_s3',
        python_callable=joined_s3
    )

    end_pipeline = DummyOperator(
        task_id='end_pipeline'
    )

    # using groups of tasks
    with TaskGroup(group_id='group_a', tooltip='etl') as group_a:

        # creating table 1: airlines
        create_table_airlines = PostgresOperator(
            task_id='create_table_airlines',
            postgres_conn_id='rds_de_postgres',
            sql='''
            CREATE TABLE IF NOT EXISTS airlines (
                name text NOT NULL,
                code_name text,
                ICAO text PRIMARY KEY
            );
            '''
        )

        # truncating table
        truncating_airlines = PostgresOperator(
            task_id='truncating_airlines',
            postgres_conn_id='rds_de_postgres',
            sql="TRUNCATE TABLE airlines;"
        )

        # importing airlines data from s3 to postgres using aws_s3 extension previously installed
        s3_to_postgres_airlines = PostgresOperator(
            task_id='s3_to_postgres_airlines',
            postgres_conn_id='rds_de_postgres',
            sql='''
            SELECT aws_s3.table_import_from_s3('airlines', '', '(format csv, DELIMITER'','', HEADER true)', 'fligths-project-datalake', 'airlines.csv', 'sa-east-1')
            '''
        )
        # creating table 2: airports

        create_table_airports = PostgresOperator(
            task_id='create_table_airports',
            postgres_conn_id='rds_de_postgres',
            sql='''
            DROP TABLE airports;
            CREATE TABLE IF NOT EXISTS airports (
                id VARCHAR(10), 
                name text NOT NULL,
                iata CHAR(3) PRIMARY KEY,
                gps_code VARCHAR(20),
                type text, 
                iso_country CHAR(2),
                iso_region VARCHAR(10),
                city text,
                local_code VARCHAR(10),
                latitude numeric, 
                longitude numeric, 
                elevation_ft numeric,
                elevation_m numeric,
                scheduled_service BOOLEAN
                
            );
            '''
        )

        # truncating table
        truncating_airports = PostgresOperator(
            task_id='truncating_airports',
            postgres_conn_id='rds_de_postgres',
            sql="TRUNCATE TABLE airports;"

        )

        # reading airports from s3
        read_airports_s3 = PythonOperator(
            task_id='read_airports_s3',
            python_callable=read_csv_from_s3
        )

        # transforming airports data task
        transform_airport = PythonOperator(
            task_id='transform_airport',
            python_callable=transform_airports
        )

        # loading airports into postgres
        load_airport_postgres = PythonOperator(
            task_id='load_airport_postgres',
            python_callable=load_to_postgres,
            op_kwargs={'taskId': 'group_a.transform_airport',
                       'table_name': 'airports'}  # passing function args
        )

        # creating table 3: flights

        create_table_flights = PostgresOperator(
            task_id='create_table_flights',
            postgres_conn_id='rds_de_postgres',
            sql='''
            DROP TABLE flights;
            CREATE TABLE IF NOT EXISTS flights(
                flight CHAR(6) PRIMARY KEY,
                airline CHAR(3) NOT NULL,
                departure CHAR(3),
                arrival CHAR(3) NOT NULL,
                altitude numeric,
                tracked_time date,
                groundspeed numeric,
                verticalspeed numeric,
                registration VARCHAR(10),
                station VARCHAR(20),
                type text,
                latitude numeric,
                longitude numeric
            );
            '''
        )

        # extracting data form an api

        extract_api_flight_data = PythonOperator(
            task_id='extract_api_flight_data',
            python_callable=extract_flight_data_api
        )

        # transforming and loading flights table to postgres
        transform_load_flights_postgres = PythonOperator(
            task_id='transform_load_flights_postgres',
            python_callable=transform_load_flights_pg
        )

        create_table_airlines >> truncating_airlines >> s3_to_postgres_airlines
        create_table_airports >> truncating_airports >> read_airports_s3 >> transform_airport >> load_airport_postgres
        create_table_flights >> extract_api_flight_data >> transform_load_flights_postgres

    start_pipeline >> group_a >> join_data >> load_joined_to_s3 >> end_pipeline
