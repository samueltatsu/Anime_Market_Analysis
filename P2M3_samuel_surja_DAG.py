'''
=================================================
Milestone 3

Nama  : Samuel Tatang Surja
Batch : HCK-012

This is DAG file to run airflow to automate a set of task:
1. Fetch data from PostgreSQL
2. Clean data
3. Post cleaned data to ElasticSearch for further analysis. 

Dataset used is collection of Anime released up to 2023.
=================================================
'''

from airflow.models import DAG
from airflow.operators.python import PythonOperator
# from airflow.providers.postgres.operators.postgres import PostgresOperator
# from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pandas as pd

from elasticsearch import Elasticsearch
# from elasticsearch.helpers import bulk

 
def fetch_data():
    '''
    This function fetch data from specific table from PostgreSQL
    '''
    # fetch data from:
    database = "airflow_m3"
    username = "airflow_m3"
    password = "airflow_m3"
    host = "postgres"

    # connecting to PostgreSQL 
    postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"

    # SQLAlchemy connection
    engine = create_engine(postgres_url)
    conn = engine.connect()
 
    df = pd.read_sql_query("select * from table_m3", conn)
    df.to_csv('/opt/airflow/dags/P2M3_samuel_surja_data_raw.csv', sep=',', index=False)
    
# Custom function to convert duration to minutes
def duration_to_seconds(duration):
    '''
    This function converts duration values from the format "h hr. m min. s sec." into seconds
    '''
    if duration == 'Unknown':
        return 0
    elif 'hr.' in duration and 'min.' in duration:
        hours = int(duration.split()[0])
        minutes = int(duration.split()[0])
        return hours * 3600 + minutes * 60
    elif 'hr.' in duration:
        hours = int(duration.split()[0])
        return hours * 3600
    elif 'min.' in duration:
        minutes = int(duration.split()[0])
        return minutes * 60
    elif 'sec.' in duration:
        seconds = int(duration.split()[0])
        return seconds

def preprocessing(): 
    ''' 
    This function is for data cleaning:
    - standardize column name
    - handle missing values
    - handle duplicates
    - handle data types
    '''
    # load data
    data = pd.read_csv("/opt/airflow/dags/P2M3_samuel_surja_data_raw.csv")

    # hard-code normalized column name
    data.columns = [
        'anime_id', 'name', 'score', 'genres', 'english_name', 'japanese_name',
        'synopsis', 'type', 'episodes', 'aired', 'premiered', 'producers',
        'licensors', 'studios', 'source', 'duration_sec_per_ep', 'rating', 'ranked',
        'popularity', 'members', 'favorites', 'watching', 'completed',
        'on_hold', 'dropped'
        ]

    # handle missing values
    data['synopsis'] = data['synopsis'].fillna("Unknown")
    data['ranked'] = data['ranked'].fillna(-1)        # set unknown rank to 0
    data.dropna(inplace=True)

    # handle duplicates
    data.drop_duplicates(inplace=True)

    # handle data types
    # episodes
    data['episodes'] = data['episodes'].str.replace('Unknown', '-1')      # set unknown total episodes as 0
    data['episodes'] = data['episodes'].astype('int64')
    # duration
    data['duration_sec_per_ep'] = data['duration_sec_per_ep'].apply(duration_to_seconds)
    data['duration_sec_per_ep'] = data['duration_sec_per_ep'].astype('int64')
    #ranked
    data['ranked'] = data['ranked'].astype('int64')

    # save data
    data.to_csv('/opt/airflow/dags/P2M3_samuel_surja_data_clean.csv', index=False)
    
def post_to_elasticsearch():
    '''
    This function is for posting the cleaned data to elasticsearch
    '''
    es = Elasticsearch("http://elasticsearch:9200")
    df = pd.read_csv('/opt/airflow/dags/P2M3_samuel_surja_data_clean.csv')
    
    for i, r in df.iterrows():
        doc = r.to_dict()  # Convert the row to a dictionary
        res = es.index(index="m3_v1", id=i+1, body=doc)
        print(f"Response from Elasticsearch: {res}")
        
    
default_args = {
    'owner': 'Samuel', 
    'start_date': datetime(2024, 2, 23, 10, 0) - timedelta(hours=7)
}

with DAG(
    "DAG_M3", 
    description='Milestone_3',
    schedule_interval='30 6 * * *', # schedule airflow to run at 06:30.
    default_args=default_args, 
    catchup=False
) as dag:
    
    # Task: 1
    fetch_data_pg = PythonOperator(
        task_id='fetch_from_postgresql',
        python_callable=fetch_data)
    
 
    # Task: 2
    edit_data = PythonOperator(
        task_id='data_cleaning',
        python_callable=preprocessing)
 
    # Task: 3
    upload_data = PythonOperator(
        task_id='post_to_elasticsearch',
        python_callable=post_to_elasticsearch)

    # airflow process
    fetch_data_pg >> edit_data >> upload_data 