from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    COPY_SQL = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        region 'us-west-2'
        BLANKSASNULL
        EMPTYASNULL
        TRIMBLANKS
        {} ;
    """
        
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 file_source_path="",
                 destination_table="",
                 aws_credentials="",
                 JSON_format="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.file_source_path = file_source_path
        self.destination_table = destination_table
        self.aws_credentials = aws_credentials
        self.JSON_format = JSON_format
        
        
       
        
    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(self.redshift_conn_id)
        redshift_hook.run("TRUNCATE TABLE  {}".format(self.destination_table))
        redshift_hook.run(StageToRedshiftOperator.COPY_SQL.format(self.destination_table,self.file_source_path, \
                                                      credentials.access_key,credentials.secret_key,self.JSON_format))





