from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pymongo
import pandas as pd
from google.cloud import bigquery
import os

# Função para extrair e converter dados do MongoDB
def extrair_e_converter(**kwargs):
    client = pymongo.MongoClient('mongodb+srv://teste:a1b2c3@bc26.amljwv1.mongodb.net/?retryWrites=true&w=majority&appName=BC26')
    db = client['bases2']
    colecao = db['aula2']

    try:
        documentos = list(colecao.find())
        if not documentos:
            print("A coleção está vazia.")
            return None

        df = pd.DataFrame(documentos)
        if '_id' in df.columns:
            df.drop(columns=['_id'], inplace=True)
        df.reset_index(drop=True, inplace=True)

        # Verificar e limpar linhas incompletas
        expected_columns = 68  # Número de colunas esperado (ajuste conforme necessário)
        df = df.dropna(thresh=expected_columns)  # Remover linhas com menos colunas do que o esperado

        # Converter colunas de data para o formato datetime
        date_columns = ['dataNascimento', 'dataExpedicao']  # Ajuste conforme suas colunas de datas
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce', format='%Y-%m-%d')  # Corrige e converte as datas

        # Salvar o DataFrame como um arquivo CSV temporário com delimitador diferente (por exemplo, ponto e vírgula)
        temp_csv_path = '/tmp/mongo_data.csv'
        df.to_csv(temp_csv_path, sep=';', index=False)
        print(f"Dados salvos em arquivo temporário: {temp_csv_path}")

        # Armazenar o caminho do arquivo no contexto do Airflow para uso na próxima tarefa
        kwargs['ti'].xcom_push(key='temp_csv_path', value=temp_csv_path)
    except Exception as e:
        print(f"Erro ao extrair e carregar dados: {e}")
        return None

# Função para enviar o DataFrame para o BigQuery
def enviar_para_bigquery(**kwargs):
    # Recuperar o caminho do arquivo CSV do XCom
    temp_csv_path = kwargs['ti'].xcom_pull(key='temp_csv_path', task_ids='extrair_e_converter')

    if not temp_csv_path or not os.path.exists(temp_csv_path):
        print("O arquivo CSV não foi encontrado. Abortando o envio para o BigQuery.")
        return

    client = bigquery.Client()
    table_id = 'plated-reducer-343721.projeto.aula'

    # Configuração do job de carregamento
    job_config = bigquery.LoadJobConfig(
        schema=[
            job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("identidadeGenero", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("tentativasTecnico", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("statusJornada", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("genero", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("deficiencia", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("dataInscricao", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("cidade", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("possuiComputador", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("cep", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("statusFinal", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("pronome", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("disponibilidadeHorario", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("laudoUrl", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("pais", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("questaoAtualLogico", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("empresaQueTrabalha", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("uid", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("tipo", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("uf", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("mediaTecnico", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("questaoAtualTecnico", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("resultsTotalTecnico", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("resultsTecnico", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("questoesTecnico", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("resultsLogico", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("resultsTotalLogico", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("mediaLogico", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("questoesLogico", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("laudoState", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("videoStatus", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("videoEnviadoEm", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("mediaDinamica", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("mediaIngles", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("lideranca", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("souColaborador", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("chapa", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("contratoStatus", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("assinaturaDistrato", "STRING", mode="NULLABLE")
    ],
        write_disposition="WRITE_TRUNCATE",  # Substitui os dados da tabela no BigQuery
        source_format=bigquery.SourceFormat.CSV,  # Formato do arquivo de origem
        skip_leading_rows=1,  # Pula a primeira linha, caso seja cabeçalho
        max_bad_records=10,  # Permite até 10 registros com erros antes de falhar
        field_delimiter=';'  # Especifica o delimitador de campo
    )

    try:
        with open(temp_csv_path, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_id, job_config=job_config)
        job.result()  # Aguarda a conclusão do job
        print("Dados enviados com sucesso para o BigQuery.")
    except Exception as e:
        print(f"Erro ao enviar dados para o BigQuery: {e}")

# Definição do DAG e operadores do Airflow
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=50),
}

dag = DAG(
    'mongodb_to_bigquery',
    default_args=default_args,
    description='Extrair dados do MongoDB e enviar para o BigQuery',
    schedule_interval='@daily',
)

extrair_e_converter_task = PythonOperator(
    task_id='extrair_e_converter',
    python_callable=extrair_e_converter,
    dag=dag,
)

enviar_para_bigquery_task = PythonOperator(
    task_id='enviar_para_bigquery',
    python_callable=enviar_para_bigquery,
    dag=dag,
)

extrair_e_converter_task >> enviar_para_bigquery_task

