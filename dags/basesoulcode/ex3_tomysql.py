from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from pymongo import MongoClient
import pandas as pd
from sqlalchemy import create_engine

def read_from_mongo():
    # Conectar ao MongoDB
    client = MongoClient('mongodb://seu_usuario:senha@host:porta/')
    db = client['nome_do_banco']
    colecao = db['nome_da_colecao']

    # Ler os dados do MongoDB
    df_teste2 = list(colecao.find({}, {"notas": 1, "id": 1, "email": 1, "_id": 0}))
    df_2 = pd.DataFrame(df_teste2)

    # Inicializar uma lista para armazenar todos os DataFrames
    df_list = []

    # Iterar diretamente sobre a série 'notas'
    for index, item in df_2['notas'].items():
        if isinstance(item, dict):
            for key, value in item.items():
                temp_df = pd.DataFrame(value)
                temp_df['chave_mongo'] = key
                temp_df['id'] = df_2['id'].iloc[index]
                df_list.append(temp_df)

    # Concatenar todos os DataFrames da lista em um único DataFrame
    novo_df = pd.concat(df_list, ignore_index=True)

    # Salvar o DataFrame como CSV temporário
    novo_df.to_csv('/tmp/arquivo_processado.csv', index=False)
    
    return '/tmp/arquivo_processado.csv'

def upload_to_mysql(**kwargs):
    # Carregar o caminho do arquivo processado
    file_path = kwargs['ti'].xcom_pull(task_ids='read_from_mongo')

    # Conectar ao MySQL
    engine = create_engine('mysql+pymysql://usuario:senha@host:porta/nome_do_banco')
    df = pd.read_csv(file_path)

    # Enviar o DataFrame para o MySQL
    df.to_sql('nome_da_tabela', con=engine, if_exists='replace', index=False)

default_args = {
    'owner': 'seu_usuario',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'dag_mongo_to_mysql',
    default_args=default_args,
    description='DAG para ler do MongoDB, processar e enviar ao MySQL',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    read_mongo_task = PythonOperator(
        task_id='read_from_mongo',
        python_callable=read_from_mongo,
    )

    upload_mysql_task = PythonOperator(
        task_id='upload_to_mysql',
        python_callable=upload_to_mysql,
        provide_context=True,
    )

    read_mongo_task >> upload_mysql_task
