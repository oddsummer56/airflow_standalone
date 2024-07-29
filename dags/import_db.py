from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator 
with DAG(
        'import-db',
    # TDummyOperatorhese args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='import db DAG',
    #schedule=timedelta(days=1),
    schedule='10 4 * * *',
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['import', 'db', 'etl'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    
    task_check = BashOperator(
            task_id="check.done",
            bash_command="bash {{var.value.CHECK_SH}} {{var.value.DONE_PATH}}/bash_done/{{ds_nodash}}/_DONE"    )
            
    task_csv = BashOperator(
            task_id="to.csv",
            bash_command="""
                echo "to.csv"
                U_PATH=~/data/count/{{ds_nodash}}/count.log
                CSV_PATH=~/data/csv/{{ds_nodash}}
                CSV_FILE=~/data/csv/{{ds_nodash}}/csv.csv

                mkdir -p $CSV_PATH

                #cat $U_PATH | awk '{print "\\"{{ds}}\\",\\"" $2 "\\",\\"" $1 "\\""}' > ${CSV_FILE}
                cat $U_PATH | awk '{print "^{{ds}}^,^" $2 "^,^" $1 "^"}' > ${CSV_FILE}
                echo $CSV_PATH
    """
    )

    task_create_table = BashOperator(
            task_id="create.table",
            bash_command="""
                SQL={{var.value.SQL_PATH}}/create_db_table.sql
                echo "SQL_PATH=$SQL"
                mysql < $SQL
            """
    )

    task_tmp = BashOperator(
            task_id="to.tmp",
            bash_command="""
                echo "to.tmp"
                CSV_FILE=~/data/csv/{{ds_nodash}}/csv.csv
                bash {{var.value.SH_HOME}}/csv2mysql.sh $CSV_FILE {{ds}} 
            """
    )

    task_base = BashOperator(
            task_id="to.base",
            bash_command="""
                echo "to base"
                bash {{var.value.SH_HOME}}/tmp2base.sh {{ds}}
                """
    )
    
    task_err = BashOperator(
            task_id="err.report",
            bash_command="""
                echo "error report"
            """,
            trigger_rule="one_failed"
    )

    task_done = BashOperator(
            task_id="make.done",
            bash_command="""
                #DONE_PATH=~/data/import_done/{{ds_nodash}}
                #mkdir -p ${DONE_PATH}
                #touch ${DONE_PATH}/_DONE
                DONE_PATH={{var.value.DONE_PATH}}/import_done/{{ds_nodash}}
                mkdir -p $DONE_PATH
                touch $DONE_PATH/_DONE

                figlet "make.done.end"
            """
    )


    task_start = EmptyOperator(task_id='start')
    task_end = EmptyOperator(task_id='end', trigger_rule="all_done")

    task_start >> task_check 
    task_check >> task_csv >> task_create_table >> task_tmp >> task_base >> task_done >> task_end
    task_check >> task_err >> task_end

