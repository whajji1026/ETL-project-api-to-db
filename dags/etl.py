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
        CREATE TABLE IF NOT EXISTS apod_data (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            explanation TEXT,
            url TEXT,
            date DATE,
            media_type VARCHAR(50)
        );
        """

        ## execute the table creation query
        postgres_hook.run(create_table_query)



    ## step 2: extract the Nasa api data (apod)
    ##https://api.nasa.gov/planetary/apod?api_key=WC7tVcDdlF1NyW8PHHbb6ATgQGKtjnYx7tLBNbRM
    ## api_key ="WC7tVcDdlF1NyW8PHHbb6ATgQGKtjnYx7tLBNbRM"
    extract_apod=SimpleHttpOperator(
        task_id='extract_apod',
        http_conn_id='nasa_api', ## connection ID defined in airflow for nasa api
        endpoint='planetary/apod', ## nasa endpoint
        method='GET',
        data={"api_key":"{{conn.nasa_api.extra_dejson.api_key}}"}, ## use api key from connection
        response_filter=lambda response:response.jason(), ## convert response to json
    )

    ## step 3: transform the data (pick the info that i need to save)
    @task 
    def transform_apod_data(response):
        apod_data={
            'title': response.get('title',''),
            'explanation': response.get('explanation',''),
            'url': response.get('url',''),
            'date': response.get('date',''),
            'media_type': response.get('media_type','')

        }
        return apod_data 

    ## step 4: load the data into Postgres SQL
    @task
    def load_data_to_postgres(apod_data):
        # initiamize the postgresHook
        postgres_hook=PostgresHook(postgres_conn_id="my_postgres_connection")
        
        ## define the SQL insert query
        insert_query = """
        INSERT INTO apod_data (title, explanation, url, date, media_type)
        VALUES (%s,%s,%s,%s,%s);
        """

        ## execute the sql query
        postgres_hook.run(insert_query,parameters=(
            apod_data['title'],
            apod_data['explanation'],
            apod_data['url'],
            apod_data['date'],
            apod_data['media_type']
        ))
        


    ## step 5 : verify the DBViewer


    ##step 6: define the task dependencies
    ###extract
    create_table() >> extract_apod ## ensure the data is created before extraction
    api_response=extract_apod.output 
    ### tarnsform
    transformed_data=transform_apod_data(api_response)
    ### load
    load_data_to_postgres(transformed_data)

