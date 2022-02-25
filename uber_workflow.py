from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.email_operator import EmailOperator

from new_uber import uber_project

print(uber_project)


default_args = {"owner": "airflow1", "start_date":datetime(2022, 2, 24), "catchup": False}
with DAG(dag_id="workflow1",default_args=default_args,schedule_interval='@daily') as dag:
    check_file = BashOperator(
        task_id="check_file_task",
        bash_command="shasum ~/ip_files/archive",
        retries=2,
        retry_delay=timedelta(seconds=15))

    pre_process = PythonOperator(
        task_id="uber_project_task_id",
        python_callable=uber_project
    )
    create_table = MySqlOperator(
        task_id="create_table",
        mysql_conn_id="mysql_db1",
        sql="create table data(Date_time datetime,lat float,lon float,Base varchar(250),month int,weekday varchar(250),day int,hour int,min int)"
    )
    """
    insert = MySqlOperator(
       task_id='insert_db',
       mysql_conn_id="mysql_db1",
       sql="LOAD DATA  INFILE '/var/lib/mysql-files/fin.csv' INTO TABLE aggre_res FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS;")
    )
    """
    check_file >> pre_process >> create_table