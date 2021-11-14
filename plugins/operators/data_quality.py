from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.checks = checks

    def execute(self, context):
        if len(self.checks) <= 0:
            self.log.info("No checks")
            return
        
        redshift_hook = PostgresHook(self.redshift_conn_id)
        error_count = 0
        error_tests = []
        
        for check in self.checks:
            sql = check.get('sql')
            expected_result = check.get('result')

            try:
                result = redshift_hook.get_records(sql)[0]
            except Exception as e:
                self.log.info(f"SQL error: {e}")

            if expected_result != result[0]:
                error_count += 1
                error_tests.append(sql)

        if error_count > 0:
            self.log.info('The data is not as expected')
            self.log.info(failing_tests)
            raise ValueError('Failed')
        else:
            self.log.info("Data quality is ok!")