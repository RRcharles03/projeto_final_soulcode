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
        colecao = db['colecao_fake_formacao']

        # Ler os dados do MongoDB
        formacao_Mongo = list(colecao.find())

        # Verifica se não existe uma coleção no mongo, caso positivo retorno uma mensagem
        # caso negativo converte o dicionário em um dataframe
        if not formacao_Mongo:
            print("Não foi possivel encontrar dados na coleção")
        else:
            df_formacao = pd.DataFrame(formacao_Mongo)

        # Salvar o DataFrame como CSV no Bucket do GCS
        tmp_csv_path = '/tmp/arquivo_formacao.csv'
        df_formacao.to_csv(tmp_csv_path, encoding = 'utf-8', index=False)

        # Armazenar o caminho do arquivo no contexto do Airflow para uso na próxima tarefa
        kwargs['ti'].xcom_push(key='tmp_csv_path', value=tmp_csv_path)

    except Exception as e :
        print(f"Erro ao extrair e tratar os dados: {e}")
        return None

# Função para tratar os dados e transformar em DataFrame
def transform_data(**kwargs):
    tmp_csv_path = kwargs['ti'].xcom_pull(key='tmp_csv_path', task_ids='extract_transform_mongo')

    if not tmp_csv_path or not os.path.exists(tmp_csv_path):
        raise FileNotFoundError("O arquivo CSV não foi encontrado. Cancelando o envio para o BigQuery.")

    # Corrigir: Carregar o CSV corretamente
    df_formacao = pd.read_csv(tmp_csv_path)

    # Padrões de substituição mapeando caracteres errados para os corretos
    padroes_substituicoes = {
        r'Ã§': 'ç', r'Ã©': 'é', r'Ã¢': 'â', r'Ã³': 'ó', r'Ã£': 'ã', r'Ã': 'í',
        r'Ã¡': 'á', r'Ãª': 'ê', r'Ã­': 'í', r'íº': 'ú', r'ã³': 'ó', r'ã©': 'é',
        r'ã§': 'ç', r'ã£': 'ã', r'ã±': 'ñ', r'ã´': 'ô', r'ãš': 'Ú', r'ãœ': 'ü',
        r'íª': 'ê', r'í´': 'ô', r'í¡': 'á', r'íµ': 'õ'
    }

    # Função para aplicar as substituições usando expressões regulares
    def corrigir_texto(texto):
        if isinstance(texto, list):
            return [corrigir_texto(item) for item in texto]
        elif isinstance(texto, str):
            for erro, correto in padroes_substituicoes.items():
                texto = re.sub(erro, correto, texto)
            return texto
        else:
            return texto

    # Função para normalizar capitalização e preencher valores nulos
    def tratar_coluna(coluna):
        coluna = coluna.apply(corrigir_texto)
        coluna = coluna.str.lower().str.title()
        return coluna.fillna('Não Informado')

    # Lista de colunas para tratar
    colunas_para_tratar = ['status', 'curso', 'instituicao', 'areaFormacao']

    # Aplicar o tratamento em todas as colunas da lista
    df_formacao[colunas_para_tratar] = df_formacao[colunas_para_tratar].apply(tratar_coluna)

     # Ajustar a data para incluir hora padrão
    df_formacao['dataInicio'] = pd.to_datetime(df_formacao['dataInicio'], errors='coerce')
    df_formacao['dataTermino'] = pd.to_datetime(df_formacao['dataTermino'], errors='coerce')

    # Adicionar hora padrão '00:00:00' para datas sem hora
    df_formacao['dataInicio'] = df_formacao['dataInicio'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(x) else None)
    df_formacao['dataTermino'] = df_formacao['dataTermino'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(x) else None)

    # Verifica se existe _id proveniente do mongo, caso positivo o mesmo é descartado
    if '_id' in df_formacao.columns:
        df_formacao.drop(columns=['_id'], inplace=True)
    df_formacao.reset_index(drop=True, inplace=True)

    # Salvar o DataFrame como CSV no Bucket do GCS
    df_formacao.to_csv(tmp_csv_path, index=False)

    # Armazena o DataFrame em XCom para a próxima etapa
    kwargs['ti'].xcom_push(key='tmp_csv_path', value=tmp_csv_path)

def upload_to_bigquery(**kwargs):
    # Recuperar o caminho do arquivo CSV do XCom
    tmp_csv_path = kwargs['ti'].xcom_pull(key='tmp_csv_path', task_ids='extract_transform_mongo')

    if not tmp_csv_path or not os.path.exists(tmp_csv_path):
        print("O arquivo CSV não foi encontrado. Cancelando o envio para o BigQuery.")
        return

    client = bigquery.Client()
    table_id = 'arcane-force-428113-v6.soulcode_projetofinal.formacao'

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("IdFormacao", "STRING"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("curso", "STRING"),
            bigquery.SchemaField("instituicao", "STRING"),
            bigquery.SchemaField("dataInicio", "TIMESTAMP"),
            bigquery.SchemaField("dataTermino", "TIMESTAMP"),
            bigquery.SchemaField("areaFormacao", "STRING")
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
    'dag_mongo_to_bq_formacao',
    default_args=default_args,
    description='DAG para ler do MongoDB, processar e enviar ao BigQuery',
    schedule_interval=dt.timedelta(days=1),
    catchup=True,
) as dag:

    extract_transform_mongo = PythonOperator(
        task_id='extract_transform_mongo',
        python_callable=extract_transform_mongo,
    )

    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    upload_bq_task = PythonOperator(
        task_id='upload_to_bigquery',
        python_callable=upload_to_bigquery,
    )

    extract_transform_mongo >> transform_data >> upload_bq_task