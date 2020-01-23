from datetime import datetime
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_plugin import PostgresRowCountOperator
from airflow.models import DAG

table_name = 'test'

with DAG(dag_id = 'db_dag', schedule_interval='* * * * *', start_date=datetime(2020, 1, 2), max_active_runs=1, is_paused_upon_creation=False) as db_dag:
    def print_info(info_msg, **kwargs):
        print(info_msg)

    print_info_op = PythonOperator(
            task_id = 'print_info_op',
            provide_context = True,
            python_callable=print_info,
            op_args = ['{{ task_instance.dag_id }} start processing tables in database: {{database}}']
    )

    echo_user_op = BashOperator(
            task_id = 'echo_user_op',
            #TODO: Replace with echo $USER
            bash_command='echo test',
            xcom_push=True
    )

    def check_table_exist(table_name):
        hook = PostgresHook()
        sql = f'''
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_name = \'{table_name}\');'''
        table_exists = hook.get_first(sql)
        if str(table_exists[0]) == 'True':
            return 'skip_table_creation_op'
        else:
            return 'create_table_op'

    branch_table_exists_op = BranchPythonOperator(
            task_id = 'branch_table_op',
            python_callable=check_table_exist,
            op_args=[table_name])

    create_table_op = PostgresOperator(
                task_id='create_table_op', 
                sql = f'''
                CREATE TABLE {table_name}(
                    custom_id SERIAL PRIMARY KEY,
                    user_name VARCHAR (50) NOT NULL,
                    timestamp TIMESTAMP NOT NULL);''')
    skip_table_creation_op = DummyOperator(task_id = 'skip_table_creation_op')

    insert_row_op = PostgresOperator(
            task_id = 'insert_row_op',
            sql = "INSERT INTO {} (user_name, timestamp) VALUES ('{{{{ task_instance.xcom_pull(task_ids = 'echo_user_op') }}}}', NOW())".format(table_name),
            trigger_rule = 'all_done')

    query_table_op = PostgresRowCountOperator(
            task_id = 'query_table_op',
            table_name = table_name)

    db_dag_push_result_op = BashOperator(
            task_id = 'db_dag_push_result_op',
            bash_command='echo "{{ task_instance.dag_id }} finished"',
            xcom_push=True
    )

    print_info_op >> echo_user_op >> branch_table_exists_op >> [create_table_op, skip_table_creation_op] >> insert_row_op >> query_table_op >> db_dag_push_result_op
