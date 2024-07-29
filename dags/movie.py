from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable

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
    
    def get_data(**kwargs):
        #print(ds)
        print(kwargs)
        print("="*20)
        print(f"ds_nodash => {kwargs['ds_nodash']}")
        print(f"kwargs type => {type(kwargs)}")
        print("="*20)
        from mov.api.call import get_key, save2df
        key = get_key()
        print(f"MOVIE_API_KEY => {key}")
        YYYYMMDD = kwargs['ds_nodash']
        df=save2df(YYYYMMDD)
        print(df.head(5))
        

    def print_context(ds=None, **kwargs):
        print("::group::All kwargs")
        pprint(kwargs)
        print(kwargs)
        print("::end group::")
        print("::group::Context variable ds")
        print(ds)
        print("::end group::")
        return "Whatever you return gets printed in the logs"

    run_this = PythonOperator( 
            task_id="print.context",
            python_callable=print_context
            )

    task_getdata = PythonOperator(
            task_id="get.data",
            python_callable=get_data
            )


    task_savedata = BashOperator(
            task_id="save.data",
            bash_command="""
                echo "save.data"

    """
    )

    task_start = gen_emp('start')
    task_end = gen_emp('end', 'all_done')
    
    task_start >> task_getdata >> task_savedata
    task_start >> run_this >> task_end

