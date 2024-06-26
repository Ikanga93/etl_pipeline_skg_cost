'''
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

def _choose_best_model(ti):
        accuracies = ti.xcom_pull(task_ids=[
                'training_model_A',
                'training_model_B',
                'training_model_C'
        ])
        best_accuracy = max(accuracies)
        if (best_accuracy > 8):
                return 'accurate'
        return 'inaccurate'

def _training_model():
        return randint(1, 10)

url = 'https://data.cdc.gov/resource/ezab-8sq5.json'

# Define the tasks
def extract_task(url):
        re = requests.get(url)
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
                python_callable=extract_task
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


with DAG("my_dag", start_date=datetime(2024, 1, 1), 
        schedule_interval="@daily", catchup=False) as dag:
    
        training_model_A = PythonOperator(
                task_id="training_model_A",
                python_callable=_training_model
        )

        training_model_B = PythonOperator(
                task_id="training_model_B",
                python_callable=_training_model
        )

        training_model_C = PythonOperator(
                task_id="training_model_C",
                python_callable=_training_model
        )

        choose_best_model = BranchPythonOperator(
                task_id="choose_best_model",
                python_callable=_choose_best_model
        )

        accurate = BashOperator(
                task_id="accurate",
                bash_command="echo 'accurate'"
        )

        inaccurate = BashOperator(
                task_id="inaccurate",
                bash_command="echo 'inaccurate'"
        )

        [training_model_A, training_model_B, training_model_C] >> choose_best_model >> [accurate, inaccurate]

'''