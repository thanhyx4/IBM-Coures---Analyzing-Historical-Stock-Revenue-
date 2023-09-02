from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
import datetime as dt

default_args = {
	'owner' : 'me',
	'start_date' : dt.datetime(2023,9,3),
    'email' : 'abc@gmail.com',
    'email_on_failure' : True,
    'email_on_retry' : True,
	'retries' : 1,
	'retry_delay'  : dt.timedelta(minutes = 5),
}
dag = DAG ('ETL_toll_data', description='Apache Airflow Final Assignment', 
default_args = default_args, schedule_interval = dt.timedelta(hours = 24) )

task_1 = BashOperator ( task_id = 'anc', 
			bash_command = 'unzip tolldata.tgz',
			dag = dag, )

task_1
