import os

from datetime import timedelta
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.models import Variable

path = Variable.get('name_path_variable', default_var='/tmp/run')
result_dir = os.path.dirname(os.path.realpath(path))

with DAG(dag_id='run_handler_dag', is_paused_upon_creation=False, max_active_runs=1, schedule_interval='* * * * *', start_date=datetime(2020, 1, 2)) as run_handler_dag:
    db_dag_sensor_op = ExternalTaskSensor(
            task_id='db_dag_sensor_op',
            external_dag_id='db_dag',
            external_task_id='db_dag_push_result_op',
            allowed_states=None,
            execution_delta=None,
            execution_date_fn=None
    )

    def print_callable(msg, **kwargs):
        print(msg)

    print_sensored_dag_result_op = PythonOperator(
           task_id='print_sensored_dag_result_op',
           python_callable=print_callable,
           provide_context=True,
           op_args=[f'{{ task_instance.xcom_pull(task_ids = db_dag_push_result_op) }}']
    )

    create_file_on_finish_op = BashOperator(
            task_id = 'create_result_on_finish_op',
            bash_command=f'touch {result_dir}/finished_{{{{ ts_nodash }}}}')

    db_dag_sensor_op >> print_sensored_dag_result_op >> create_file_on_finish_op

run_watcher_dag_args = {'schedule_interval': '@hourly', 'start_date': datetime(2020, 1, 2)}

with DAG(dag_id = 'run_watcher_dag', max_active_runs=1, start_date=datetime(2020, 1, 2), schedule_interval='* * * * *', is_paused_upon_creation=False) as run_watcher_dag:


    file_watcher_op = FileSensor(
            task_id='file_watcher_op',
            filepath=path
    )


    trigger_file_handler_dag_op = TriggerDagRunOperator(
            task_id='trigger_file_handler_dag_op',
            trigger_dag_id=run_handler_dag.dag_id
    )

    remove_file_op = BashOperator(
            task_id='remove_file_op',
            bash_command=f'rm {path}'
    )

    file_watcher_op >> trigger_file_handler_dag_op >> remove_file_op
