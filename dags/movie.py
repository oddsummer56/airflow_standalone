from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.models import Variable
from airflow.operators.python import (
    ExternalPythonOperator,
    PythonOperator,
    PythonVirtualenvOperator,
    is_venv_installed,
)

def gen_emp(id, rule="all_success"):
    op = EmptyOperator(task_id=id, trigger_rule=rule)
    return op

with DAG(
        'movie',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    max_active_runs=1,
    max_active_tasks=2,
    description='movie',
    schedule="10 4 * * *",
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['movie'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    
    def get_data(ds_nodash):
        from mov.api.call import save2df
        df=save2df(ds_nodash)
        print(df.head(5))
    
    def save_data(ds_nodash):
        from mov.api.call import apply_type2df

        df = apply_type2df(load_dt=ds_nodash)
        print(df.head(10))
        print("*"*33)
        print(df.dtypes)
       
       # 개봉일 기준 그룹핑 누적 관개수 합
       print("개봉일 기준 그룹핑 누적 관객수 합")
       g = df.groupby('openDt')
       sum_df = g.agg({'audCnt': 'sum'}).reset_index()
       print(sum_df)

    def branch_fun(**kwargs):
        ld  = kwargs['ds_nodash']
        import os
        home_dir = os.path.expanduser("~")
        path = os.path.join(home_dir, f"tmp/test_parquet/load_dt={ld}")
        print(path)
        
        if os.path.exists(path):
            return "rm.dir" #task_id
        else:
            return "get.data" , "echo.task"

    branch_op = BranchPythonOperator(
            task_id="branch.op",
            python_callable=branch_fun
            )


    task_getdata = PythonVirtualenvOperator(
            task_id="get.data",
            python_callable=get_data,
            requirements=["git+https://github.com/oddsummer56/movie.git@0.3/api"],
            system_site_packages=False,
            trigger_rule = "all_done",
            venv_cache_path="/home/oddsummer/tmp/airflow_venv/get_data"
            )


    task_savedata = PythonVirtualenvOperator(
            task_id="save.data",
            python_callable=save_data,
            requirements=["git+https://github.com/oddsummer56/movie.git@0.3/api"],
            system_site_packages=False,
            trigger_rule = "one_success",
            venv_cache_path="/home/oddsummer/tmp/airflow_venv/get_data"
    )

    rm_dir = BashOperator(
            task_id="rm.dir",
            bash_command='rm -rf ~/tmp/test_parquet/load_dt={{ds_nodash}}',
    )

    echo_task = BashOperator(
            task_id='echo.task',
            bash_command="echo 'task'"
    )

    task_start = gen_emp('start')
    task_end = gen_emp('end', 'all_done')
    join_task = BashOperator(
            task_id='join',
            bash_command="exit 1",
            trigger_rule="all_done"
    )

    task_start >> join_task >> task_savedata
    task_start >> branch_op >> echo_task >>task_savedata
    branch_op >> rm_dir >> task_getdata 
    branch_op >> task_getdata
    task_getdata >> task_savedata 
    task_savedata >> task_end

