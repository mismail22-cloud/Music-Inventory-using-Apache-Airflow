from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 test_cases=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.test_cases=test_cases
        
        
    def execute(self, context):
        error_count = 0
        failing_tests = []
        for case in self.test_cases:
            redshift_hook = PostgresHook(self.redshift_conn_id)
            sql_stmnt=case.get('sql_check')
            expct_rslt=case.get('expct_rslt')
            records = redshift_hook.get_records(sql_stmnt)[0]
            if records[0] == expct_rslt:
                logging.info("Test case Passed") 
            else:
                logging.info("Test case Failed")
                failing_tests.append(case.get('sql_check'))
                error_count+=1
        if error_count > 0:
            raise ValueError('Data quality check failed')
        
         