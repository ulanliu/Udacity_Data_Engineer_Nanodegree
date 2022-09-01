from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CreateTableOperator(BaseOperator):
    ui_color = '#800000'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 *args, **kwargs):
        
        super(CreateTableOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Creating tables")
        
        with open("/home/workspace/airflow/create_tables.sql", 'r') as f:
            content = f.read()
            
        sql_commands = content.split(';')
        
        for sql_command in sql_commands:
            redshift.run(sql_command)
        
        