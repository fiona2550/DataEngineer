from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 redshift_conn_id = '',
                 sql_query = '',
                 table = '',
                 append = True, 
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.table = table
        self.append = append
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        dimentions_sql = "INSERT INTO {} ({})".format(self.table,self.sql_query)
        redshift.run(dimentions_sql)
        if self.append:
            redshift.run("DELETE FROM {}".format(self.table))
        redshift.run(dimentions_sql)  
