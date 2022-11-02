import os
import datetime as dt
import pandas as pd
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.telegram.operators.telegram import TelegramOperator


TELEGRAM_CHAT_ID = '665659220'
TELEGRAM_CONNECTION_ID = ''

default_args = {
    'owner': 'gb',
    'start_date': dt.datetime(2022, 10, 31),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1)
}


def get_path(filename):
    return os.path.join(os.path.expanduser('~'), filename)


def download_titanic_dataset():
    url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
    df = pd.read_csv(url)
    df.to_csv(get_path('titanic.csv'), encoding='utf-8')


def pivot_dataset():
    titanic_df = pd.read_csv(get_path('titanic.csv'))
    df = titanic_df.pivot_table(index=['Sex'],
                                columns=['Pclass'],
                                values='Name',
                                aggfunc='count').reset_index()
    df.to_csv(get_path('titanic_pivot.csv'))


def mean_fare_per_class():
    titanic_df = pd.read_csv(get_path('titanic.csv'))
    fare = titanic_df[['Fare', 'Pclass']].groupby('Pclass').mean('Fare')
    fare.to_csv(get_path('titanic_mean_fares.csv'))


with DAG(
        dag_id='titanic_03',
        schedule_interval=None,
        default_args=default_args,
) as dag:
    first_task = BashOperator(
        task_id='Start',
        bash_command='echo "Here we start! run_id={{ run_id }} | dag_run={{ dag_run }}"'
    )

    last_task = BashOperator(
        task_id='Finish',
        bash_command=''' echo "Pipeline finished! Execution date is {{ execution_date.strftime('%Y-%m-%d') }}" '''
    )

    download_dataset = BashOperator(
        task_id='download_dataset',
        bash_command='''FILENAME="titanic.csv" && \
            wget https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv -O ${FILENAME} && \
            docker exec 395bb336e5bf hdfs dfs -mkdir -p /datasets  && \
            docker exec 395bb336e5bf hdfs dfs -put ${FILENAME} /datasets/  && \
            rm ${FILENAME} && \
            echo "/datasets/${FILENAME}" '''
    )

    load_data_to_hive = HiveOperator(
        task_id='load_data_to_hive',
        hql='''LOAD DATA INPATH {{ task_instance.xcom_pull(task_ids='download', key='return_value') }} INTO TABLE tablename'''
    )

    with TaskGroup("make_tables_calc_values") as make_tables:
        drop_hive_table_pivot = HiveOperator(
            task_id='drop_hive_table_pivot',
            hql='DROP TABLE IF EXISTS titanic_pivot;'
        )

        drop_hive_table_mean_fares = HiveOperator(
            task_id='drop_hive_table_mean_fares',
            hql='DROP TABLE IF EXISTS titanic_mean_fares;'
        )

        create_hive_table_mean_fares = HiveOperator(
            task_id='create_hive_table_mean_fares',
            hql='''CREATE TABLE IF NOT EXISTS titanic_pivot (Pclass INT, Fare FLOAT) 
                   ROW FORMAT DELIMITED
                   FIELDS TERMINATED BY ","
                   STORED AS TEXTFILE
                   TBLPROPERTIES('skip.header.line.count'='1');
                   '''
        )

        create_hive_table_pivot = HiveOperator(
            task_id='create_hive_table_pivot',
            hql='''CREATE TABLE IF NOT EXISTS titanic_mean_fares (Sex TEXT, class01 INT, class02 INT, class03 INT) 
                   ROW FORMAT DELIMITED
                   FIELDS TERMINATED BY ","
                   STORED AS TEXTFILE
                   TBLPROPERTIES('skip.header.line.count'='1');
                   '''
        )

        pivot_titanic_dataset = PythonOperator(
            task_id='pivot_titanic_dataset',
            python_callable=pivot_dataset,
        )

        mean_fares_titanic_dataset = PythonOperator(
            task_id='mean_fares_titanic_dataset',
            python_callable=mean_fare_per_class,
        )

        drop_hive_table_pivot >> create_hive_table_pivot >> pivot_titanic_dataset
        drop_hive_table_mean_fares >> create_hive_table_mean_fares >> mean_fares_titanic_dataset

    with TaskGroup("telegram_notice") as telegram_notice:
        show_titanic_mean_fares = BashOperator(
            task_id='show_titanic_mean_fares',
            bash_command='''docker exec 395bb336e5bf /opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "SELECT * FROM titanic_mean_fares LIMIT 5" | tr "\n" ";" '''
        )

        show_titanic_pivot = BashOperator(
            task_id='show_titanic_pivot',
            bash_command='''docker exec 395bb336e5bf /opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "SELECT * FROM titanic_pivot LIMIT 5" | tr "\n" ";" '''
        )


        def format_message(**kwargs):
            flat_text = str(kwargs['task_instance'].xcom_pull(task_ids='show_titanic_mean_fares',
                                                              key='return_value')) + ';;;' + \
                        str(kwargs['task_instance'].xcom_pull(task_ids='show_titanic_pivot',
                                                              key='return_value'))
            text = flat_text.replace(';', '\n')
            return kwargs['task_instance'].xcom_push(key='telegram_msg', value=text)


        prepare_message = PythonOperator(
            task_id='prepare_message',
            python_callable=format_message,
        )

        send_result = TelegramOperator(
            task_id='send_success_msg_telegram',
            telegram_conn_id=TELEGRAM_CONNECTION_ID,
            chat_id=TELEGRAM_CHAT_ID,
            text='''Pipeline {{ execution_date.int_timestamp }} is done. Result: {{ task_instance.xcom_pull(task_ids='prepare_message', key='telegram_msg') }}''',
        )

        [show_titanic_mean_fares, show_titanic_pivot] >> prepare_message >> send_result

    first_task >> download_dataset >> make_tables >> load_data_to_hive >> telegram_notice >> last_task






