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
        
    @task
    def get_places():
        try:
            mssql_hook = MsSqlHook(SQLSERVER_LOCALHOST)

            data = mssql_hook.get_pandas_df("select * from [CartaoConsumo].[dbo].[DimEstabelecimento]")

            return data
        except Exception as e:
            return {'error': str(e)}
    
    @task
    def import_places(data):
        try:
            pgsql_hook = PostgresHook(POSTGRES_LOCALHOST)
            engine = pgsql_hook.get_sqlalchemy_engine()

            data.rename(columns={
                'EstabelecimentoID': 'estabelecimento_id',
                'Nome': 'nome',
                'Categoria': 'categoria',
                'Cidade': 'cidade',
                'Estado': 'estado'
            }, inplace=True)

            data.to_sql(
                name='dim_estabelecimento',
                schema='etl',
                con=engine,
                if_exists='append',
                index=False
            )

            return {'status': 'success', 'records_inserted': len(data)}
        except Exception as e:
            return {'error': str(e)}
        
    @task
    def get_transaction_type():
        try:
            mssql_hook = MsSqlHook(SQLSERVER_LOCALHOST)

            data = mssql_hook.get_pandas_df("select * from [CartaoConsumo].[dbo].[DimTipoTransacao]")

            return data
        except Exception as e:
            return {'error': str(e)}
    
    @task
    def import_transaction_type(data):
        try:
            pgsql_hook = PostgresHook(POSTGRES_LOCALHOST)
            engine = pgsql_hook.get_sqlalchemy_engine()

            data.rename(columns={
                'TipoTransacaoID': 'tipo_transacao_id',
                'Descricao': 'descricao'
            }, inplace=True)

            data.to_sql(
                name='dim_tipo_transacao',
                schema='etl',
                con=engine,
                if_exists='append',
                index=False
            )

            return {'status': 'success', 'records_inserted': len(data)}
        except Exception as e:
            return {'error': str(e)}

    @task
    def get_date():
        try:
            mssql_hook = MsSqlHook(SQLSERVER_LOCALHOST)

            data = mssql_hook.get_pandas_df("select * from [CartaoConsumo].[dbo].[DimData]")

            return data
        except Exception as e:
            return {'error': str(e)}
        
    @task
    def import_date(data):
        try:
            pgsql_hook = PostgresHook(POSTGRES_LOCALHOST)
            engine = pgsql_hook.get_sqlalchemy_engine()

            data.rename(columns={
                'DataID': 'data_id',
                'Data': 'data',
                'Ano': 'ano',
                'Mes': 'mes',
                'Dia': 'dia',
                'DiaSemana': 'dia_semana'
            }, inplace=True)

            data.to_sql(
                name='dim_data',
                schema='etl',
                if_exists='append',
                con=engine,
                index=False
            )
            
            return {'status': 'success', 'record_inserted': len(data)}

        except Exception as e:
            return {'error': str(e)}

    users_data = get_users()
    places_data = get_places()
    transaction_type_data = get_transaction_type()
    date_data = get_date()
    start >> import_users(users_data) >> import_places(places_data) >> import_transaction_type(transaction_type_data) >> import_date(date_data) >> end

dag = init()