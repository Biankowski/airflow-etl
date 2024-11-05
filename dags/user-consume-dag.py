from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook


SQLSERVER_LOCALHOST='SQLSERVER_LOCALHOST'

default_args = {
    'owner': 'rodrigo',
}

@dag(
    dag_id='user-consume-dag',
    start_date=datetime(2024, 11, 4),
    max_active_runs=1,
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args,
    tags=['incremental']
)
def init():
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    @task
    def get_users():
        try:
            mssql_hook = MsSqlHook(SQLSERVER_LOCALHOST)

            data = mssql_hook.get_pandas_df("select * from [CartaoConsumo].[dbo].[DimCliente]")

            return data
        
        except Exception as e:
            return {'error': str(e)}
    
    start >> get_users() >> end

dag = init()