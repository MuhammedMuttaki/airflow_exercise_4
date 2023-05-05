# interaction with postgres database in airflow
from airflow import DAG 
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.postgres_operator import PostgresOperator 

default_args = { 
    'owner': 'airflow',
    'retries': 1, 
    'retry_delay': timedelta(minutes=5), }

create_query = """ 
DROP TABLE IF EXISTS table_1
CREATE TABLE IF NOT EXISTS table_1 (id INT NOT NULL, name VARCHAR(250) NOT NULL, age INT NOT NULL)
"""

insert_data_query = """ INSERT INTO table_1 (id, name, age) 
VALUES(1, Muhammed, 23), (2, Ali, 22), (3, Hakan, 25) """

calculating_averag_age = """ SELECT avg(age) FROM table_1 """

dag_postgres = DAG(dag_id = "postgres_dag_connection", default_args = default_args, schedule_interval = None, start_date = days_ago(1))

create_table = PostgresOperator(task_id = "creation_of_table", sql = create_query, dag = dag_postgres, postgres_conn_id = "postgres_my_local")
insert_data = PostgresOperator(task_id = "insertion_of_data", sql = insert_data_query, dag = dag_postgres, postgres_conn_id = "postgres_my_local")
group_data = PostgresOperator(task_id = "calculating_averag_age", sql = calculating_averag_age, dag = dag_postgres, postgres_conn_id = "postgres_my_local")

create_table >> insert_data >> group_data