import pandas as pd
import pymongo
import datetime as dt
import os
import re

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from pymongo import MongoClient
from google.cloud import bigquery

def extract_transform_mongo(**kwargs):
    try:
        # Conectar ao MongoDB
        client = pymongo.MongoClient('mongodb+srv://admin:admin1103@projetosoulcode.hjpkc.mongodb.net/?retryWrites=true&w=majority&appName=projetosoulcode')

        # # Acessar o banco de dados e a coleção.
        db = client['projeto_soulcode']
        colecao = db['colecao_fake_notas']

        # Ler os dados do MongoDB
        notas_Mongo = list(colecao.find())

        # Verifica se não existe uma coleção no mongo, caso positivo retorno uma mensagem
        # caso negativo converte o dicionário em um dataframe
        if not notas_Mongo:
            print("Não foi possivel encontrar dados na coleção")
        else:
            df_notas = pd.DataFrame(notas_Mongo)

        # inserir o tratamento dos dados
        df_notas['dataInscricao'] = pd.to_datetime(df_notas['dataInscricao'], errors= 'coerce', format = '%Y/%m/%d')

        if '_id' in df_notas.columns:
          df_notas.drop(columns=['_id'], inplace=True)
        df_notas.reset_index(drop=True, inplace=True)

        # Salvar o DataFrame como CSV no Bucket do GCS
        tmp_csv_path = '/tmp/arquivo_processado.csv'
        df_notas.to_csv(tmp_csv_path, index=False)


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
    table_id = 'eastern-robot-428113-c6.soulcode_projetofinal.notas'

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("IdNotas", "STRING"),
            bigquery.SchemaField("nota", "FLOAT"),
            bigquery.SchemaField("dataInscricao", "TIMESTAMP"),
            bigquery.SchemaField("observacoes", "STRING"),
            bigquery.SchemaField("disciplina", "STRING"),
            bigquery.SchemaField("criteriosAvaliacao", "STRING"),
            bigquery.SchemaField("estudanteNome", "STRING"),
            bigquery.SchemaField("professorNome", "STRING"),
            bigquery.SchemaField("mediaGeral", "STRING"),
            bigquery.SchemaField("mediaFinal", "STRING")
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
    'start_date': dt.datetime(2024,9,5),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'dag_mongo_to_bq_notas',
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