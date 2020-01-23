import logging

from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators import BaseOperator
from airflow.plugins_manager import AirflowPlugin


class PostgresRowCountOperator(BaseOperator):

    @apply_defaults
    def __init__(self, table_name, *args, **kwargs):
        self.table_name = table_name 
        super(PostgresRowCountOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        result = PostgresHook().get_first(f'''SELECT COUNT(*) FROM {self.table_name};''')
        return result

class PostgresPlugin(AirflowPlugin):
    name = 'postgres_plugin'
    operators = [PostgresRowCountOperator]
