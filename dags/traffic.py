from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
with DAG(
    'traffic',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': True,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='traffic  DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['traffic'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = BashOperator(
        task_id='getData',
        bash_command='sleep 5',
    )


    t2 = DummyOperator(task_id='data1')
    t3 = DummyOperator(task_id='data2')
    t21 = DummyOperator(task_id='station_max')
    t22 = DummyOperator(task_id='time_max')
    t31 = DummyOperator(task_id='time_traffic')

    task_info = DummyOperator(task_id='info')
    task_recommend = DummyOperator(task_id='recommend')
    task_start = DummyOperator(task_id='start')
    task_end = DummyOperator(task_id='end')
    
    task_start >> t1
    t1 >> [t2, t3] 
    t2 >> [t21, t22] 
    [t21, t22, t31] >> task_info >> task_end
    [t22, t31] >> task_recommend >> task_end

