import os
from airflow.models import DAG
from airflow import AirflowException
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from io import StringIO
import datetime as dt
import ast
import boto3
import copy
import json
import pytz
import math
import os
import pandas as pd
import random
import re
import requests
import subprocess
import time
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

# if you want  to give config  manually give like this - {"start_date" : "2024-03-15", "end_date" : "2024-03-16"}
#you will only get 2024-march-15 counts

SLACK_CONN_ID = 'ari_airflow_jobs'
def task_fail_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg = """
            :red_circle: Task Failed.
            *Task*: {task}
            *Dag*: {dag}
            *Execution Time*: {exec_date}
            *Log Url*: {log_url}
            """.format(
            task=context.get('task_instance').task_id,
            dag=context.get('task_instance').dag_id,
            ti=context.get('task_instance'),
            exec_date=context.get('run_date'),
            log_url=context.get('task_instance').log_url,
        )
    failed_alert = SlackWebhookOperator(
        task_id='slack_test',
        http_conn_id= SLACK_CONN_ID,
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='airflow')
    return failed_alert.execute(context=context)



def task_success_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg = """
            :white_check_mark: Task Succeeded.
            *Task*: {task}
            *Dag*: {dag}
            *Execution Time*: {exec_date}
            *Log Url*: {log_url}
            """.format(
            task=context.get('task_instance').task_id,
			dag=context.get('task_instance').dag_id,
            ti=context.get('task_instance'),
            exec_date=context.get('run_date'),
            log_url=context.get('task_instance').log_url,
        )
    succeeded_alert = SlackWebhookOperator(
        task_id='slack_test',
        http_conn_id=SLACK_CONN_ID,
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='airflow')
    return succeeded_alert.execute(context=context)




default_args = {
    'delay_days': 2,
    'num_days': 2,
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 21),
    'email': ['shashwat@azira.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'pool': 'roy_morgan_daily_count',
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=580),
    'on_failure_callback': task_fail_slack_alert,
    'on_success_callback': task_success_slack_alert,
    'provide_context': True,
    'time_zone':'Asia/Kolkata'
    
}

dag = DAG('Roy_Morgan_Daily_Count',
           default_args=default_args,
          schedule_interval='0 1 * * *', 
          catchup=False)





#----------------"for start and end dates"





DATE_FORMAT = "%Y-%m-%d"

def init_step(**context):
    """ Define start and end date strings in UTC. """
    c_ti = context['task_instance']
    params = context['params']
    tz_cust = pytz.timezone(params['time_zone'])
    config = context['dag_run'].conf
    if config is None:
        config = {}
    print('Manual run configuration:', config)

    # Determine start and end dates at customer location
    days_to_end = int(config.get('num_days', params['num_days']))

    if 'start_date' in config:
        start_date = tz_cust.localize(
            dt.datetime.strptime(config['start_date'], "%Y-%m-%d")
        )
    else:
        today_cust = dt.datetime.now(tz_cust).replace(hour=0, minute=0, second=0, microsecond=0)
        print('Current date at feed location:', today_cust)
        delay_days = int(params['delay_days'])
        start_date = today_cust - dt.timedelta(
            days=(delay_days + days_to_end - 1)
        )

    end_date = start_date + dt.timedelta(
        days=days_to_end,
        seconds=-1
    )

    print('Date ranges are from : {} to {}'.format(
        start_date,
        end_date
    ))
    c_ti.xcom_push(key="start_date", value=start_date.strftime(DATE_FORMAT))
    c_ti.xcom_push(key="end_date", value=end_date.strftime(DATE_FORMAT))


init_op = PythonOperator(
    task_id='init_step',
    python_callable=init_step,
    op_kwargs={'params': default_args},
    provide_context=True,
    dag=dag
)


# spark configuaration:


spark_conf = {
     'spark.yarn.maxAppAttempts':2,
     'spark.sql.files.ignoreCorruptFiles':'true',
     'spark.sql.parquet.fs.optimized.committer.optimization-enabled':'true',
     'spark.sql.broadcastTimeout':'10000',
     'spark.dynamicAllocation.minExecutors':1,
     'spark.dynamicAllocation.maxExecutors':25,
     'spark.dynamicAllocation.enabled':'false',
     'spark.network.timeout':10000000,
     'spark.executor.heartbeatInterval':'600',
     'spark.driver.cores':2,
     'spark.shuffle.io.retryWait':60,
     'spark.shuffle.io.maxRetries':10,
     'spark.serializer':'org.apache.spark.serializer.KryoSerializer',
     'repositories': 'https://repos.spark-packages.org',
     'packages': 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,qubole:sparklens:0.3.2-s_2.11',
     'spark.mongodb.input.uri': 'mongodb://da-user:7fDT90x0kebO@nv01-prod-common-mongo09.adnear.net/compass.campaigns',
     'spark.executor.extraClassPath':'/etc/hadoop/conf:/etc/hive/conf:/usr/lib/hadoop-lzo/lib/*:/usr/share/aws/aws-java-sdk/*:/usr/share/aws/emr/emrfs/conf:/usr/share/aws/emr/emrfs/lib/*:/usr/share/aws/emr/emrfs/auxlib/*',
     'spark.driver.extraClassPath':'/etc/hadoop/conf:/etc/hive/conf:/usr/lib/hadoop-lzo/lib/*:/usr/share/aws/aws-java-sdk/*:/usr/share/aws/emr/emrfs/conf:/usr/share/aws/emr/emrfs/lib/*:/usr/share/aws/emr/emrfs/auxlib/*',
     'spark.jars.packages': 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1'

      }



#submiting spark file to dag



spark_op = SparkSubmitOperator(
       task_id = 'submit_spark',
       conn_id = "spark_default",
       name = 'sojern_daily_count',
       application = '/opt/airflow/scripts/shashwat/roy_morgan.py',
       py_files="/home/hadoop/common/util.py",
       num_executors = 15,
       executor_cores = 4,
       executor_memory = '10G',
       driver_memory = '5G',
       driver_cores = 2,
       conf=spark_conf,
       application_args=['--start_date', '{{ task_instance.xcom_pull(task_ids="init_step", key="start_date") }}',
                         '--end_date', '{{ task_instance.xcom_pull(task_ids="init_step", key="end_date") }}'],
       dag = dag )



#check wheather it writen or not---------


def checking_file_s3_bucket(**context):
    start_date = context['ti'].xcom_pull(task_ids='init_step', key='start_date')
    end_date = context['ti'].xcom_pull(task_ids='init_step', key='end_date')
    range_folder = "{}_{}".format(start_date.replace("-", ""), end_date.replace("-", ""))
    print(f"checking for this folder range {range_folder}")
    s3_folder = f'shashwat/sojern_pixel_counts/final_output/{range_folder}/'
    
    # Check if file exists in S3 bucket
    s3 = boto3.client('s3')
    objects = s3.list_objects_v2(Bucket='staging-near-data-analytics', Prefix=s3_folder)
    
    # If objects list is not empty, file is generated
    if 'Contents' in objects and len(objects['Contents']) > 0:
        return f"File for date range {start_date} to {end_date} is generated in S3 bucket."
    else:
        return f"No file is generated in S3 bucket for date range {start_date} to {end_date}."




check_file_op = PythonOperator(
    task_id='check_file_generated_in_s3_bucket',
    python_callable=checking_file_s3_bucket,
    provide_context=True,
    dag=dag
)







#appending to google sheet if exist 


def append_to_sheet(**context):
    try:
        SLACK_CONN_ID = 'ari_airflow_jobs'
        start_date = context['ti'].xcom_pull(task_ids='init_step', key='start_date')
        end_date = context['ti'].xcom_pull(task_ids='init_step', key='end_date')
        range_folder = "{}_{}".format(start_date.replace("-", ""), end_date.replace("-", ""))
        s3_folder = f'shashwat/sojern_pixel_counts/final_output/{range_folder}/'

        # List objects in the S3 folder
        s3 = boto3.client('s3')
        objects = s3.list_objects_v2(Bucket='staging-near-data-analytics', Prefix=s3_folder)

        def is_csv_file(key):
            return key.lower().endswith('.csv')
        csv_files = [obj['Key'] for obj in objects['Contents'] if is_csv_file(obj['Key']) and not obj['Key'].endswith('_SUCCESS')]

        ##        csv_files = [obj['Key'] for obj in objects['Contents'] if is_csv_file(obj['Key'])]
        if len(csv_files) == 0:
            print(f"No CSV file is generated in S3 bucket for date range {start_date} to {end_date}.")
            return


        # Retrieve the latest object
        latest_object = max((obj for obj in objects['Contents'] if is_csv_file(obj['Key'])), key=lambda x: x['LastModified'])

        #latest_object = max(objects['Contents'], key=lambda x: x['LastModified'])
        csv_file_key = latest_object['Key']
        print(f"Latest CSV file: {csv_file_key}")
        # Read the CSV file from S3
        obj = s3.get_object(Bucket='staging-near-data-analytics', Key=csv_file_key)
        csv_content = obj['Body'].read().decode('utf-8')
        print("zero")
        print(f"CSV file content: {obj['Body'].read()}")
        print(f"Length of CSV content: {len(csv_content)}")
        data = pd.read_csv(StringIO(csv_content))
        #data = pd.read_csv(obj['Body'])
        print(data)
        total_impressions = data.iloc[:, -1].sum()
        print(total_impressions)
        slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
        slack_msg = f":white_check_mark: Data appended to Google Sheet successfully. Total Impressions for {start_date} : {total_impressions}"
        slack_alert = SlackWebhookOperator(
            task_id='slack_test',
            http_conn_id=SLACK_CONN_ID,
            webhook_token=slack_webhook_token,
            message=slack_msg,
            username='airflow'
        )
        slack_alert.execute(context=context)
        
        print(data)
        print("one")
        print("CSV file contents:")
        print("three")
        values = []
        for index, row in data.iterrows():
            row_values = []
            for value in row:
                if pd.isna(value):
                    row_values.append('')  # Convert NaN to empty string
                #elif value == "$INSERTION_ORDER_ID":
                 #   row_values.append("$INSERTION_ORDER_ID")
                #elif value == "{$INSERTION_ORDER_ID}":
                    #row_values.append("INSERTION_ORDER_ID")
                else:
                    row_values.append(value)
            values.append(row_values)

        print("Data appended to Google Sheet successfully.")


        # Authenticate with the service account credentials
        creds = service_account.Credentials.from_service_account_file(
            '/opt/airflow/scripts/shashwat/gsheets-dagmonitor-2ed58df3287c.json',
            scopes=['https://www.googleapis.com/auth/spreadsheets']
        )

        # Build the Sheets API service
        service = build('sheets', 'v4', credentials=creds)

        # Specify the Google Sheet ID and sheet name
        spreadsheet_id = '1iAMxnVRrrNqYgHVi2aEpA_sEQNsCnoxSZbJJsz28J-U'
        sheet_name = 'daily_count'

        # Calculate the next empty row for appending
        try:
            sheet = service.spreadsheets()
            result = sheet.values().get(spreadsheetId=spreadsheet_id, range=sheet_name).execute()
            num_rows = len(result.get('values', []))
            next_row = num_rows + 1
            start_row = f'{sheet_name}!A{next_row}'
        except HttpError as e:
            start_row = f'{sheet_name}!A1'  # If the sheet is empty, start from the first row

        # Call the Sheets API to append the data
        service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=start_row,
            valueInputOption='RAW',
            body={'values': values},
            insertDataOption='INSERT_ROWS',
            includeValuesInResponse=True,
            responseValueRenderOption='FORMATTED_VALUE',
            responseDateTimeRenderOption='FORMATTED_STRING',
        ).execute()

        print("Data appended to Google Sheet successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")



# Define PythonOperator to call append_to_sheet function
append_to_sheet_op = PythonOperator(
    task_id='append_to_sheet',
    python_callable=append_to_sheet,
    provide_context=True,
    dag=dag
)








init_op >> spark_op >> check_file_op >> append_to_sheet_op