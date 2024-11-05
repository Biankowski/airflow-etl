from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


SQLSERVER_LOCALHOST='SQLSERVER_LOCALHOST'
POSTGRES_LOCALHOST='POSTGRES_LOCALHOST'

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
    
    @task
    def import_users(data):
        try:
            pgsql_hook = PostgresHook(POSTGRES_LOCALHOST)

            engine = pgsql_hook.get_sqlalchemy_engine()

            data.rename(columns={
                'ClienteID': 'cliente_id',
                'Nome': 'nome',
                'Idade': 'idade',
                'Genero': 'genero',
                'RendaMensal': 'renda_mensal'
            }, inplace=True)

            data.to_sql(
                name='dim_cliente',
                schema='etl',
                con=engine,
                if_exists='append',
                index=False,
            )

            return {'status': 'success', 'records_inserted': len(data)}
        except Exception as e:
            return {'error': str(e)}

    users_data = get_users()
    start >> import_users(users_data) >> end

dag = init()