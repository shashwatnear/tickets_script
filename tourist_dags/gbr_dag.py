import airflow
import os
import sys
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
#from airflow.providers.amazon.aws.operators.batch import BatchCreateComputeEnvironmentOperator, BatchOperator, BaseOperator
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator


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
            exec_date=context.get('execution_date'),
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
            exec_date=context.get('execution_date'),
            log_url=context.get('task_instance').log_url,
        )
    succeeded_alert = SlackWebhookOperator(
        task_id='slack_test',
        http_conn_id=SLACK_CONN_ID,
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='airflow')
    return succeeded_alert.execute(context=context)


# Following are defaults which can be overridden later on
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 20),
    'email': ['shashwat@azira.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=580),
    'on_failure_callback': task_fail_slack_alert,
    'on_success_callback': task_success_slack_alert,
    'schedule_interval': '@daily',
}

dag = DAG('gbr_inbound_monthly', 
          default_args=default_args,
          schedule_interval='0 12 25 * *', 
          max_active_runs=1, 
          catchup=False)

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
     'spark.executor.extraClassPath':'/etc/hadoop/conf:/etc/hive/conf:/usr/lib/hadoop-lzo/lib/*:/usr/share/aws/aws-java-sdk/*:/usr/share/aws/emr/emrfs/conf:/usr/share/aws/emr/emrfs/lib/*:/usr/share/aws/emr/emrfs/auxlib/*',
     'spark.driver.extraClassPath':'/etc/hadoop/conf:/etc/hive/conf:/usr/lib/hadoop-lzo/lib/*:/usr/share/aws/aws-java-sdk/*:/usr/share/aws/emr/emrfs/conf:/usr/share/aws/emr/emrfs/lib/*:/usr/share/aws/emr/emrfs/auxlib/*',

      }
spark_conn_id = 'spark_default'


t1 = SparkSubmitOperator(
       task_id = 'monthly_refresh_tourists_data_greatbritain',
       conn_id = spark_conn_id,
       name = 'monthly_refresh_tourists_data_greatbritain',
       application = '/opt/airflow/scripts/shashwat/monthly_refresh_tourists_data_greatbritain.py',
       application_args = [

       ],
       num_executors = 12,
       executor_cores = 2,
       executor_memory = '14G',
       driver_memory = '4G',
       driver_cores = 2,
       conf=spark_conf,
       dag = dag )