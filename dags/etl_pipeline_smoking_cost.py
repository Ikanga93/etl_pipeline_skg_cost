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
# url = 'https://data.cdc.gov/resource/ezab-8sq5.json'
# url = 'http://api.census.gov/data/timeseries/poverty/histpov2'
url = 'https://api.census.gov/data/timeseries/poverty/histpov2?get=United States,2022&for=us:*&time=2022&key=9ee34b3a8cc8c17f2087157f0359df008602fc61'
csv_path = '/Users/jbshome/Desktop/airflow_docker/smoking_cost.csv'
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

extracted_data = extract_task('https://api.census.gov/data/timeseries/poverty/histpov2?get=United States,2022&for=us:*&time=2022&key=9ee34b3a8cc8c17f2087157f0359df008602fc61')
# print(data)

def transform_task(data):
        try:
                # Convert the json_data to a pandas dataframe
                df = pd.DataFrame(data)
                logging.info('Data transformed successfuly')

                # Fix column names
                df = df.rename(columns={'locationabbr': 'location_abbr', 
                                        'locationdesc': 'location_desc', 
                                        'datasource': 'data_source', 
                                        'topictype': 'topic_type', 
                                        'submeasureid': 'sub_measure_id', 
                                        'displayorder': 'display_order'})
                logging.info('Columns renamed successfully')

                # Configure pandas to display all rows and columns
                
                pd.set_option('display.max_rows', None)
                pd.set_option('display.max_columns', None)
                pd.set_option('display.width', None)
                # pd.set_option('display.max_colwidth', None)
                
                return df
        except Exception as e:
                logging.error('Failed to transform data')
                return None

cleaned_data = transform_task(extracted_data)
print(cleaned_data)

def load_task(data):
        data.to_csv('smoking_cost.csv', index=False)
        logging.info("Successfully loaded")

# Call the function
load_task(cleaned_data)

# Define the Dag
with DAG("etl_pipeline_smoking_cost", start_date=datetime(2024, 1, 1),
        schedule_interval="@yearly", catchup=False) as dag:

        extract_task_01 = PythonOperator(
                task_id="extract_task_01",
                python_callable=extract_task,
                op_kwargs={'url':'https://api.census.gov/data/timeseries/poverty/histpov2?get=United States,2022&for=us:*&time=2022&key=9ee34b3a8cc8c17f2087157f0359df008602fc61'}
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

