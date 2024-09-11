import pandas as pd
import pymongo
import datetime as dt
import numpy as np
import os
import re

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from pymongo import MongoClient
from google.cloud import bigquery
from dateutil import parser

def extract_transform_mongo(**kwargs):
    try:
        # Conectar ao MongoDB
        client = pymongo.MongoClient('mongodb+srv://teste:a1b2c3@bc26.amljwv1.mongodb.net/?retryWrites=true&w=majority&appName=BC26')

        # # Acessar o banco de dados e a coleção.
        db = client['bases']
        colecao = db['aula1']

        # Ler os dados do MongoDB
        data_Mongo = list(colecao.find())

        # Verifica se não existe uma coleção no mongo, caso positivo retorno uma mensagem
        # caso negativo converte o dicionário em um dataframe
        if not data_Mongo:
            print("Não foi possivel encontrar dados na coleção")
        else:
            df = pd.DataFrame(data_Mongo)

        # Lista para armazenar todos os DataFrames temporários
        df_list_empregabilidade = []

        # Iterar diretamente sobre a série 'empregabilidadeUpdateBy' no DataFrame
        for index, item in df[['empregabilidadeUpdateBy', 'id']].iterrows():
            empregabilidade_item = item['empregabilidadeUpdateBy']
            empregabilidade_id = item['id']

            # Verificar se o item é uma lista não vazia
            if isinstance(empregabilidade_item, list) and len(empregabilidade_item) > 0:
                for sub_item in empregabilidade_item:
                    if isinstance(sub_item, dict):
                        temp_df = pd.DataFrame([sub_item])
                        temp_df['id'] = empregabilidade_id
                        df_list_empregabilidade.append(temp_df)
            # Verificar se o item é um dicionário
            elif isinstance(empregabilidade_item, dict):
                temp_df = pd.DataFrame([empregabilidade_item])
                temp_df['id'] = empregabilidade_id
                df_list_empregabilidade.append(temp_df)

        # Concatenar todos os DataFrames da lista em um único DataFrame, se df_list_empregabilidade não estiver vazio
        if df_list_empregabilidade:
            df_empregabilidade = pd.concat(df_list_empregabilidade, ignore_index=True)
        else:
            df_empregabilidade = pd.DataFrame()  # Cria um DataFrame vazio se df_list_empregabilidade estiver vazio

        # Substituir strings vazias e espaços em branco por NaN em todo o DataFrame
        df_empregabilidade = df_empregabilidade.replace(r'^\s*$', np.nan, regex=True)

        # Salvar o DataFrame como CSV no Bucket do GCS
        tmp_csv_path = '/tmp/arquivo_empregabilidade.csv'
        df_empregabilidade.to_csv(tmp_csv_path, index=False)

        # Armazenar o caminho do arquivo no contexto do Airflow para uso na próxima tarefa
        kwargs['ti'].xcom_push(key='tmp_csv_path', value=tmp_csv_path)

    except Exception as e :
        print(f"Erro ao extrair e tratar os dados: {e}")
        return None

def upload_to_bigquery(**kwargs):
    # Recuperar o caminho do arquivo CSV do XCom
    tmp_csv_path = kwargs['ti'].xcom_pull(key='tmp_csv_path', task_ids='extract_transform_mongo')

    if not tmp_csv_path or not os.path.exists(tmp_csv_path):
        print("O arquivo CSV não foi encontrado. Cancelando o envio para o BigQuery.")
        return

    client = bigquery.Client()
    table_id = 'arcane-force-428113-v6.soulcodemongo.empregabilidade'

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("type", "STRING"),
            bigquery.SchemaField("uid", "STRING"),
            bigquery.SchemaField("id", "STRING"),
        ],

        write_disposition="WRITE_TRUNCATE",  # Substitui os dados da tabela no BigQuery
        skip_leading_rows=1, # Pula a primeira linha, caso seja cabeçalho
        source_format=bigquery.SourceFormat.CSV # Formato do arquivo de origem
    )

    try:
        with open(tmp_csv_path, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_id, job_config=job_config)
        job.result()  # Espera a conclusão do job
        print("Job realizado com sucesso, dados enviados para a Bigquery")
    except Exception as e :
        print(f"Erro ao realizar o job: {e}")

# Definição da DAG e operadores do Airflow
default_args = {
    'owner': 'projeto_soulcode',
    'depends_on_past': False,
    'start_date': dt.datetime(2024,9,9),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'dag_mongo_to_bq_empregabilidade',
    default_args=default_args,
    description='DAG para ler do MongoDB, processar e enviar ao BigQuery',
    schedule_interval=dt.timedelta(days=1),
    catchup=True,
) as dag:

    extract_transform_mongo = PythonOperator(
        task_id='extract_transform_mongo',
        python_callable=extract_transform_mongo,
    )

    upload_bq_task = PythonOperator(
        task_id='upload_to_bigquery',
        python_callable=upload_to_bigquery,
    )

    extract_transform_mongo >> upload_bq_task