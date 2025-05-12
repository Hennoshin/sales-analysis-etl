"""
=================================================
Milestone 3

Name: Muhammad Irfan Hilmi
Batch: FTDS 026

This program is created to perform ETL (extract, transform, load) of the dataset data from PostgreSQL,
cleaning the data, and then loading it into Elasticsearch
=================================================
"""

import pandas as pd
from datetime import datetime

from sqlalchemy import create_engine

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator

from elasticsearch import Elasticsearch, helpers


default_args= {
    'owner': 'Irfan',
    'start_date': datetime(2024, 11, 1)
}

with DAG(
    'etl_data',
    description='Extract from Postgres, transform, and load to Elasticsearch',
    schedule_interval='10,20,30 9 * * SAT',
    default_args=default_args, 
    catchup=False) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    @task()
    def read_data_from_postgres():
        """
        This function runs the task of fetching the data from PostgreSQL.
        SQLAlchemy is used to create a connection to the database.

        All the data is then retrieved and saved into a .csv file to be processed for the next task.
        """
        database = "airflow"
        username = "airflow"
        password = "airflow"
        host = "postgres"
        port = 5432

        connection_url = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"

        engine = create_engine(connection_url)
        conn = engine.connect()

        df = pd.read_sql("SELECT * FROM table_m3", conn)

        df.to_csv("/opt/airflow/data/extracted.csv", index=False)

    @task
    def clean_data():
        """
        This function runs the task of cleaning the data fetched from PostgreSQL.

        There are a few cleaning done in this process, such as dropping unnecassary column, dropping duplicates and null values,
        and also normalizing the column names into a snake case.

        The result of this process is saved to .csv file to be used in the next task.
        """
        df = pd.read_csv("/opt/airflow/data/extracted.csv")

        df.drop("Year-Month", axis=1, inplace=True)

        df.drop_duplicates(inplace=True)
        df.dropna(inplace=True)

        df.rename(columns={"Order ID": "Order Id"}, inplace=True)
        df.columns = df.columns.str.replace("[- ]", "", regex=True).str.replace(r"(?<!^)[A-Z]", lambda match: "_" + match.group(0), regex=True).str.lower()

        df.to_csv("/opt/airflow/data/P2M3_Irfan_Hilmi_data_clean.csv", index_label="id")

    @task
    def load_to_elastic():
        """
        This function performs the task of loading the cleaned data into Elasticsearch
        """
        df = pd.read_csv("/opt/airflow/data/P2M3_Irfan_Hilmi_data_clean.csv", index_col="id")

        es = Elasticsearch("http://elasticsearch:9200")

        def generate_actions(df, index):
            for _, row in df.iterrows():
                yield {
                    "_index": index,
                    "_source": row.to_json()
                }
        
        helpers.bulk(es, generate_actions(df, "sales_data"))

    start >> read_data_from_postgres() >> clean_data() >> load_to_elastic() >> end