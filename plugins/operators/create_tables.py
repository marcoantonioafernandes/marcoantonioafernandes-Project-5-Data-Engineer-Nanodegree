from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CreateTablesRedshiftOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 sql_stmt="",
                 *args, **kwargs):
        super(CreateTablesRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_stmt = sql_stmt

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        formatted_sql = CreateTablesRedshiftOperator.insert_sql.format(
            self.table,
            self.sql_stmt
        )
        
        self.log.info("Table created in redshift")
        redshift.run(formatted_sql)