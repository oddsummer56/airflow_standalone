from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable

def gen_emp(id, rule="all_success"):
    op = EmptyOperator(task_id=id, trigger_rule=rule)
    return op

with DAG(
        'make_parquet',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    max_active_runs=1,
    max_active_tasks=2,
    description='history log 2 mysql db',
    schedule="10 4 * * *",
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['simple', 'bash', 'etl', 'shop', "db", "history", "parquet"],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    
    task_check = BashOperator(
            task_id="check.done",
            bash_command="bash {{var.value.CHECK_SH}} {{var.value.DONE_PATH}}/import_done/{{ds_nodash}}/_DONE"
    )
            
    task_parquet = BashOperator(
           task_id="to.parquet",
            bash_command="""
                echo "to.parquet"
                READ_PATH=~/data/csv/{{ds_nodash}}/csv.csv
                SAVE_PATH=~/data/parquet/
                python ~/airflow/py/csv2parquet.py $READ_PATH $SAVE_PATH
    """
    )


    task_done = BashOperator(
            task_id="make.done",
            bash_command="""
                DONE_PATH={{var.value.DONE_PATH}}/parquet_done/{{ds_nodash}}
                mkdir -p $DONE_PATH
                touch $DONE_PATH/_DONE

                figlet "make.done.end"
            """
    )


    task_start = gen_emp('start')
    task_end = gen_emp('end', 'all_done')
    
    task_start >> task_check >> task_parquet >> task_done >> task_end 

