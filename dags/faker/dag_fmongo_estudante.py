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
        colecao = db['colecao_fake_estudante']

        # Ler os dados do MongoDB
        estudante_Mongo = list(colecao.find())

        # Verifica se não existe uma coleção no mongo, caso positivo retorno uma mensagem
        # caso negativo converte o dicionário em um dataframe
        if not estudante_Mongo:
            print("Não foi possivel encontrar dados na coleção")
        else:
            df_estudante = pd.DataFrame(estudante_Mongo)

        # inserir o tratamento dos dados
        def tratar_documentos(df, colunas):
          for coluna in colunas:
           df[coluna] = df[coluna].apply(lambda x: x.replace('.', '').replace('-', '') if isinstance(x, str) else x)
          return df

        # Colunas a serem tratadas
        colunas_para_tratar = ['cpf', 'cep']

        # Aplicando a função ao dataframe
        df_estudante = tratar_documentos(df_estudante, colunas_para_tratar)

        df_estudante['dataNascimento'] = pd.to_datetime(df_estudante['dataNascimento'], errors='coerce')

        # Adicionar hora padrão '00:00:00' para datas sem hora
        df_estudante['dataNascimento'] = df_estudante['dataNascimento'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(x) else None)

        if '_id' in df_estudante.columns:
            df_estudante.drop(columns=['_id'], inplace=True)
        df_estudante.reset_index(drop=True, inplace=True)

        # Salvar o DataFrame como CSV no Bucket do GCS
        tmp_csv_path = '/tmp/arquivo_estudante.csv'
        df_estudante.to_csv(tmp_csv_path, encoding = 'utf-8', index=False)


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
    table_id = 'arcane-force-428113-v6.soulcode_projetofinal.estudante'

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("IdEstudante", "STRING"),
            bigquery.SchemaField("IdFormacao", "STRING"),
            bigquery.SchemaField("IdNotas", "STRING"),
            bigquery.SchemaField("IdBootcamp", "STRING"),
            bigquery.SchemaField("nome", "STRING"),
            bigquery.SchemaField("nacionalidade", "STRING"),
            bigquery.SchemaField("foto", "STRING"),
            bigquery.SchemaField("statusEmpregabilidade", "STRING"),
            bigquery.SchemaField("statusNoBootcamp", "STRING"),
            bigquery.SchemaField("cidade ", "STRING"),
            bigquery.SchemaField("escolaridade", "STRING"),
            bigquery.SchemaField("cep", "STRING"),
            bigquery.SchemaField("deficiencia", "STRING"),
            bigquery.SchemaField("nivelIngles", "STRING"),
            bigquery.SchemaField("rua", "STRING"),
            bigquery.SchemaField("filhos", "INTEGER"),
            bigquery.SchemaField("cpf", "STRING"),
            bigquery.SchemaField("complemento", "STRING"),
            bigquery.SchemaField("observacoes", "STRING"),
            bigquery.SchemaField("genero", "STRING"),
            bigquery.SchemaField("estadoCivil", "STRING"),
            bigquery.SchemaField("objetivos", "STRING"),
            bigquery.SchemaField("nomeSocial", "STRING"),
            bigquery.SchemaField("disponibilidadedeMudanca", "STRING"),
            bigquery.SchemaField("telefone", "STRING"),
            bigquery.SchemaField("uf", "STRING"),
            bigquery.SchemaField("tipoDeficiencia", "STRING"),
            bigquery.SchemaField("bairro", "STRING"),
            bigquery.SchemaField("etnia", "STRING"),
            bigquery.SchemaField("dataNascimento", "TIMESTAMP"),
            bigquery.SchemaField("idiomas", "STRING"),
            bigquery.SchemaField("experienciaProfissional", "STRING"),
            bigquery.SchemaField("email", "STRING"),
            bigquery.SchemaField("cid", "STRING"),
            bigquery.SchemaField("arquivoCid", "STRING")
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
    'dag_mongo_to_bq_estudante',
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