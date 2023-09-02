from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
import datetime as dt

default_args = {
	'owner' : 'me',
	'start_date' : dt.datetime(2023,9,2, 17, 18),
    'email' : 'abc@gmail.com',
    'email_on_failure' : True,
    'email_on_retry' : True,
	'retries' : 1,
	'retry_delay'  : dt.timedelta(minutes = 5),
}
dag = DAG ('ETL_toll_data', description='Apache Airflow Final Assignment', 
default_args = default_args, schedule_interval = dt.timedelta(hours = 24) )

task_1 = BashOperator ( task_id = 'unzip_data', 
			bash_command = 'tar -xvzf /home/project/airflow/dags/tolldata.tgz',
			dag = dag, )

task_1
