# Import libraries
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from random import randint
from datetime import datetime
from airflow.operators.bash import BashOperator
import requests
import logging
import pandas as pd
import pendulum


url = 'https://data.cdc.gov/resource/ezab-8sq5.json'

# Define the tasks
def extract_task(api_url):
        re = requests.get(api_url)
        json_data = re.json()
        return json_data

def transform_task(data):
        # Convert the json_data to a pandas dataframe
        df = pd.DataFrame(data)
        return df

def load_task(data):
        pass

# Define the Dag
with DAG("etl_pipeline_smoking_cost", start_date=datetime(2024, 1, 1),
        schedule_interval="@daily", catchup=False) as dag:

        extract_task_01 = PythonOperator(
                task_id="extract_task_01",
                python_callable=extract_task,
                op_kwargs={'url':'https://data.cdc.gov/resource/ezab-8sq5.json'}
        )

        transform_task_01 = PythonOperator(
                task_id="transform_task_01",
                python_callable=transform_task
        )

        load_task_01 = PythonOperator(
                task_id="load_task_01",
                python_callable=load_task
        )

        extract_task_01 >> transform_task_01 >> load_task_01

