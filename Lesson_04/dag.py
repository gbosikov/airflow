import datetime as dt
import pandas as pd
from airflow.models import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.decorators import task

default_args = {
    'owner': 'gb',
    'start_date': dt.datetime(2022, 11, 11),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1)
}

with DAG(
        dag_id='taskflow_api_titanic',
        schedule_interval=None,
        default_args=default_args,
) as dag:
    @task()
    def first_task(*args):
        print(f'Here we start!')
        return True


    @task()
    def download_dataset(*args):
        url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
        df = pd.read_csv(url)
        return df.to_json()


    @task()
    def make_tables(*args):
        pg_hook = PostgresHook()
        sql = f'''
            CREATE TABLE IF NOT EXISTS {Variable.get("table_pivot")}(                
                Sex Text,
                class01 INT, 
                class02 INT, 
                class03 INT
            );
            TRUNCATE TABLE {Variable.get("table_pivot")};

            CREATE TABLE IF NOT EXISTS {Variable.get("table_mean_fare")}(
                pclass INT NULL, 
                fare numeric NOT NULL
            );    
            TRUNCATE TABLE {Variable.get("table_mean_fare")};    
        ''',
        pg_hook.run(sql)


    @task()
    def pivot_dataset(titanic_json: dict) -> dict:
        return pd.read_json(titanic_json).pivot_table(
            index=['Sex'],
            columns=['Pclass'],
            values='Name',
            aggfunc='count'
        ).reset_index().to_dict(orient='records')


    @task()
    def mean_fare_dataset(titanic_json: dict) -> dict:
        return pd.read_json(titanic_json) \
            .groupby('Pclass')['Fare'] \
            .mean().reset_index() \
            .to_dict(orient='records')


    @task()
    def load_table_pivot(data: dict):
        pg_hook = PostgresHook()
        pg_insert = f'INSERT INTO {Variable.get("table_pivot")} (sex, class01, class02, class03) VALUES (%s, %s, %s, %s)'
        for row in data:
            pg_hook.run(pg_insert, parameters=(row['Sex'], row['1'], row['2'], row['3'],))
        return True


    @task()
    def load_table_mean_fare(data: dict):
        pg_hook = PostgresHook()
        pg_insert = f'INSERT INTO {Variable.get("table_mean_fare")} (pclass, fare) VALUES (%s, %s)'
        for row in data:
            pg_hook.run(pg_insert, parameters=(row['Pclass'], row['Fare'],))
        return True


    @task()
    def last_task(*args):
        print('Finish!')
        return True


    ds = download_dataset(make_tables(first_task()))

    load_1 = load_table_pivot(pivot_dataset(ds))
    load_2 = load_table_mean_fare(mean_fare_dataset(ds))

    last_task(load_1, load_2)