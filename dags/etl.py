from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
from airflow .providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import json


## define the dag

with DAG(
    dag_id='nasa_apod_postgres',
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False 
) as dag:
    ## step 1: create the table if it doesnt exist
    
    @task
    def create_table():
        ##initialize teh postgreshook
        postgres_hook=PostgresHook(postgres_conn_id="my_postgres_connection")

        ## SQL query to create the table
        create_table_query="""



        """




    ## step 2: extract the Nasa api data (apod)


    ## step 3: transform the data (pick the info that i need to save)


    ## step 4: load the data into Postgres SQL


    ## step 5 : verify the DBViewer


    ##step 6: define the task dependencies


