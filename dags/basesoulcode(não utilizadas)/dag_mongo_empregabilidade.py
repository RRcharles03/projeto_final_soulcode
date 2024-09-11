import pandas as pd
import pymongo

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from pymongo import MongoClient
from google.cloud import bigquery

def read_from_mongo():
    # Conectar ao MongoDB
    client = pymongo.MongoClient('mongodb+srv://teste:a1b2c3@bc26.amljwv1.mongodb.net/?retryWrites=true&w=majority&appName=BC26')
    db = client['bases']
    colecao = db['aula1']

    df_Empregabilidade_Mongo = list(colecao.find({}, {"empregabilidadeUpdateBy": 1, "id": 1, "_id": 0}))
    df_Empregabilidade = pd.DataFrame(df_Empregabilidade_Mongo)

    # Inicializar uma lista para armazenar todos os DataFrames
    df_list = []

    # Iterar diretamente sobre a série 'empregabilidadeUpdateBy'
    for index, item in df_Empregabilidade['empregabilidadeUpdateBy'].items():
        # Verificar se o item é uma lista não vazia
        if isinstance(item, list) and len(item) > 0:
            for sub_item in item:
                if isinstance(sub_item, dict):
                    temp_df = pd.DataFrame([sub_item])
                    temp_df['id'] = df_Empregabilidade['id'].iloc[index]
                    df_list.append(temp_df)
        # Verificar se o item é um dicionário
        elif isinstance(item, dict):
            temp_df = pd.DataFrame([item])
            temp_df['id'] = df_Empregabilidade['id'].iloc[index]
            df_list.append(temp_df)

    # Concatenar todos os DataFrames da lista em um único DataFrame, se df_list não estiver vazio
    if df_list:
        novo_df = pd.concat(df_list, ignore_index=True)
    else:
        novo_df = pd.DataFrame()  # Cria um DataFrame vazio se df_list estiver vazio

    # Salvar o DataFrame como CSV no Bucket do GCS
    novo_df.to_csv('/tmp/arquivo_processado.csv', index=False)

def upload_to_bigquery():
    client = bigquery.Client()
    table_id = 'eastern-robot-428113-c6.soulcode_projetofinal.empregabilidade'

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("type", "STRING"),
            bigquery.SchemaField("uid", "STRING"),
            bigquery.SchemaField("id", "STRING"),
            # Adicione os outros campos do esquema
        ],
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
    )

    with open('/tmp/arquivo_processado.csv', "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)

    job.result()  # Espera a conclusão do job

default_args = {
    'owner': 'seu_usuario',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'dag_mongo_to_bq_empregabilidade',
    default_args=default_args,
    description='DAG para ler do MongoDB, processar e enviar ao BigQuery',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    read_mongo_task = PythonOperator(
        task_id='read_from_mongo',
        python_callable=read_from_mongo,
    )

    upload_bq_task = PythonOperator(
        task_id='upload_to_bigquery',
        python_callable=upload_to_bigquery,
    )

    read_mongo_task >> upload_bq_task
