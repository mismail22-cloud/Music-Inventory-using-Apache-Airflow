from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 insert_statement="",
                 destination_table="",
                 load_mode="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.insert_statement = insert_statement
        self.destination_table = destination_table
        self.load_mode = load_mode

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.load_mode == 'Truncate_Load':
            truncate_statement=f'TRUNCATE TABLE {self.destination_table}'
            insert_statement = self.insert_statement.format(destination_table=self.destination_table)
            redshift_hook.run(truncate_statement)
            redshift_hook.run(insert_statement)
        else:
            insert_statement = self.insert_statement.format(destination_table=self.destination_table)
            redshift_hook.run(insert_statement)

