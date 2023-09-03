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
default_args = default_args, schedule_interval = dt.timedelta(hours=5) )

task_1 = BashOperator ( task_id = 'unzip_data', 
			bash_command = 'tar -xvzf /home/project/airflow/dags/tolldata.tgz',
			dag = dag, )

task_2 = BashOperator( task_id = 'extract_data_from_csv', 
            bash_command = 'cut -d \',\' -f1,2,3,4 /home/project/airflow/dags/vehicle-data.csv > /home/project/airflow/dags/csv_data.csv', 
            dag = dag,)

task_3 = BashOperator( task_id = 'extract_data_from_tsv', 
            bash_command = 'cut -f5,6,7 /home/project/airflow/dags/tollplaza-data.tsv > /home/project/airflow/dags/tsv_data.csv', 
            dag = dag,)

task_4 = BashOperator( task_id = 'extract_data_from_fixed_width', 
            bash_command = 'cut -f6,7 /home/project/airflow/dags/payment-data.txt > /home/project/airflow/dags/fixed_width_data.csv', 
            dag = dag,)

task_5 = BashOperator( task_id = 'consolidate_data',
            bash_command= '',
            dag= dag,)
