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

# Logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
url = 'https://data.cdc.gov/resource/ezab-8sq5.json'

# Define the tasks
def extract_task(url):
        try:
                re = requests.get(url)
                json_data = re.json()
                logging.info('Data extracted successfully')
                return json_data
        except requests.exceptions.RequestException as e:
                logging.error('Failed to extract data')
                return None

# data = extract_task('https://data.cdc.gov/resource/ezab-8sq5.json')

def transform_task(data):
        try:
                # Convert the json_data to a pandas dataframe
                df = pd.DataFrame(data)
                logging.info('Data transformed successfuly')
                return df
        except Exception as e:
                logging.error('Failed to transform data')
                return None

        # Configure pandas to display all rows and columns
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', None)


# print(transform_task(data))

def load_task(data):
        pass

# Define the Dag
with DAG("etl_pipeline_smoking_cost", start_date=datetime(2024, 1, 1),
        schedule_interval="@yearly", catchup=False) as dag:

        extract_task_01 = PythonOperator(
                task_id="extract_task_01",
                python_callable=extract_task,
                op_kwargs={'url':'https://data.cdc.gov/resource/ezab-8sq5.json'}
        )

        transform_task_01 = PythonOperator(
                task_id="transform_task_01",
                python_callable=transform_task,
                op_args=['{{ ti.xcom_pull(task_ids="extract_task_01") }}']
        )

        load_task_01 = PythonOperator(
                task_id="load_task_01",
                python_callable=load_task,
                op_args=['{{ ti.xcom_pull(task_ids="transform_task_01") }}']
        )

        extract_task_01 >> transform_task_01 >> load_task_01

