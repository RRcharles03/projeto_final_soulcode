import pandas as pd
import pymongo
import datetime as dt
import os

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from pymongo import MongoClient
from google.cloud import bigquery

def read_from_mongo(**kwargs):
    try: 
        # Conectar ao MongoDB
        client = pymongo.MongoClient('mongodb+srv://teste:a1b2c3@bc26.amljwv1.mongodb.net/?retryWrites=true&w=majority&appName=BC26')
        db = client['bases']
        colecao = db['aula1']

        # Ler os dados do MongoDB
        Notas_Mongo = list(colecao.find({}, {"notas": 1, "id": 1,  "_id": 0}))
        if not Notas_Mongo:
            print("Não foi possivel encontrar dados na coleção")
        else:                
            df_Notas = pd.DataFrame(Notas_Mongo)

        # Inicializar uma lista para armazenar todos os DataFrames
        df_list = []

        # Iterar diretamente sobre a série 'notas'
        for index, item in df_Notas['notas'].items():
            if isinstance(item, dict):
                for key, value in item.items():
                    temp_df = pd.DataFrame(value)
                    temp_df['chave mongo'] = key
                    temp_df['id'] = df_Notas['id'].iloc[index]
                    df_list.append(temp_df)

        # Concatenar todos os DataFrames da lista em um único DataFrame
        novo_df = pd.concat(df_list, ignore_index=True)

        # Substituir NaN por 'não informado' (se apropriado)
        novo_df['professor'].fillna('não informado', inplace=True)
        novo_df['professorNome'].fillna('não informado', inplace=True)

        # Tratar a coluna 'nota'
        novo_df['nota'] = novo_df['nota'].replace('Não informado', float(0))  # Substitui 'Não informado' por NaN
        novo_df['nota'] = pd.to_numeric(novo_df['nota'], errors='coerce')  # Converte para numérico, forçando NaN para valores não convertíveis

        # Opcional: preencher NaN com 0
        novo_df['nota'].fillna(0, inplace=True)  # Substitui NaN por 0

        # Substituindo strings vazias por um valor específico
        novo_df.replace('', 'não informado', inplace=True)

        # Salvar o DataFrame como CSV no Bucket do GCS
        tmp_csv_path = '/tmp/arquivo_processado.csv'
        novo_df.to_csv(tmp_csv_path, index=False)


        # Armazenar o caminho do arquivo no contexto do Airflow para uso na próxima tarefa
        kwargs['ti'].xcom_push(key='tmp_csv_path', value=tmp_csv_path)

    except Exception as e :
        print(f"Erro ao extrair e tratar os dados: {e}")
        return None

def upload_to_bigquery(**kwargs):
    # Recuperar o caminho do arquivo CSV do XCom
    tmp_csv_path = kwargs['ti'].xcom_pull(key='tmp_csv_path', task_ids='read_from_mongo')

    if not tmp_csv_path or not os.path.exists(tmp_csv_path):
        print("O arquivo CSV não foi encontrado. Cancelando o envio para o BigQuery.")
        return
 
    client = bigquery.Client()
    table_id = 'eastern-robot-428113-c6.soulcode_projetofinal.notas'

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("nota", "FLOAT"),
            bigquery.SchemaField("data_milissegundos", "STRING"),
            bigquery.SchemaField("observacoes", "STRING"),
            bigquery.SchemaField("professorId", "STRING"),
            bigquery.SchemaField("atividade", "STRING"),
            bigquery.SchemaField("criterios", "STRING"),
            bigquery.SchemaField("disciplina", "STRING"),
            bigquery.SchemaField("atividadeId", "STRING"),
            bigquery.SchemaField("estudanteNome", "STRING"),
            bigquery.SchemaField("professorNome", "STRING"),
            bigquery.SchemaField("chave mongo", "STRING"),
            bigquery.SchemaField("id", "STRING"),
            bigquery.SchemaField("professor", "STRING"),
            # Adicione os outros campos do esquema
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
    'owner': 'soulcode',
    'depends_on_past': False,
    'start_date': dt.datetime(2024,9,1),
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

# Original
# with DAG(
#     'dag_mongo_to_bq_notas',
#     default_args=default_args,
#     description='DAG para ler do MongoDB, processar e enviar ao BigQuery',
#     schedule_interval='@daily',
#     catchup=False,
# ) as dag:

    read_mongo_task = PythonOperator(
        task_id='read_from_mongo',
        python_callable=read_from_mongo,
    )

    upload_bq_task = PythonOperator(    
        task_id='upload_to_bigquery',
        python_callable=upload_to_bigquery,
    )

    read_mongo_task >> upload_bq_task