import pandas as pd
import pymongo
import datetime as dt

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from pymongo import MongoClient
from google.cloud import bigquery

# Está DAG não será utilizada 

def read_from_mongo():
    # Conectar ao MongoDB
    client = pymongo.MongoClient('mongodb+srv://teste:a1b2c3@bc26.amljwv1.mongodb.net/?retryWrites=true&w=majority&appName=BC26')
    db = client['bases']
    colecao = db['aula1']

    # Ler os dados do MongoDB
    df = list(colecao.find())
    novo_df = pd.DataFrame(df)

    # Removendo pontos e hífens.
    novo_df['cpf'] = novo_df['cpf'].apply(lambda x: x.replace('.', '').replace('-', '') if isinstance(x, str) else x)

    # Salvar o DataFrame como CSV no Bucket do GCS
    novo_df.to_csv('/tmp/arquivo_processado.csv', index=False)

def upload_to_bigquery():
    client = bigquery.Client()
    table_id = 'eastern-robot-428113-c6.soulcode_projetofinal.aula1'

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("_id", "STRING"),
            bigquery.SchemaField("numero", "STRING"),
            bigquery.SchemaField("foto", "STRING"),
            bigquery.SchemaField("id", "STRING"),
            bigquery.SchemaField("statusEmpregabilidade", "STRING"),
            bigquery.SchemaField("statusNoBootcamp", "STRING"),
            bigquery.SchemaField("cidade", "STRING"),
            bigquery.SchemaField("escolaridadeNivel", "STRING"),
            bigquery.SchemaField("CEP", "STRING"),
            bigquery.SchemaField("nivelIngles", "STRING"),
            bigquery.SchemaField("rua", "STRING"),
            bigquery.SchemaField("filhos", "STRING"),
            bigquery.SchemaField("inglesInicial", "STRING"),
            bigquery.SchemaField("cpf", "STRING"),
            bigquery.SchemaField("parceiroAssociado", "STRING"),
            bigquery.SchemaField("complemento", "STRING"),
            bigquery.SchemaField("bootcampNome", "STRING"),
            bigquery.SchemaField("observacoes", "STRING"),
            bigquery.SchemaField("genero", "STRING"),
            bigquery.SchemaField("bootcampTipo", "STRING"),
            # bigquery.SchemaField("notas", "STRING"),
            bigquery.SchemaField("estadoCivil", "STRING"),
            bigquery.SchemaField("objetivos", "STRING"),
            bigquery.SchemaField("nomeSocial", "STRING"),
            bigquery.SchemaField("disponibilidadeMudanca", "STRING"),
            bigquery.SchemaField("uf", "STRING"),
            bigquery.SchemaField("bootcampCodigo", "STRING"),
            bigquery.SchemaField("tipoDeficiencia", "STRING"),
            bigquery.SchemaField("dataDoBatch", "STRING"),
            bigquery.SchemaField("areaFormacao", "STRING"),
            bigquery.SchemaField("mediaGeral", "STRING"),
            bigquery.SchemaField("bairro", "STRING"),
            bigquery.SchemaField("favorito", "STRING"),
            bigquery.SchemaField("etnia", "STRING"),
            bigquery.SchemaField("dataNascimento", "STRING"),
            bigquery.SchemaField("idiomas", "STRING"),
            bigquery.SchemaField("formacaoAcademica", "STRING"),
            bigquery.SchemaField("experienciaProfissional", "STRING"),
            bigquery.SchemaField("mediaFinal", "STRING"),
            bigquery.SchemaField("email", "STRING"),
            bigquery.SchemaField("cep", "STRING"),
            bigquery.SchemaField("graduation", "STRING"),
            bigquery.SchemaField("statusAuth", "STRING"),
            bigquery.SchemaField("uid", "STRING"),
            bigquery.SchemaField("estudanteId", "STRING"),
            # bigquery.SchemaField("formacaoAcademicaList", "STRING"),
            bigquery.SchemaField("selfLearner", "STRING"),
            # bigquery.SchemaField("historico", "STRING"),
            # bigquery.SchemaField("empregabilidadeUpdateBy", "STRING"),
            bigquery.SchemaField("empregabilidadeLastUpdate", "STRING"),
            bigquery.SchemaField("nomeDoCursoSuperior", "STRING"),
            bigquery.SchemaField("cid", "STRING"),
            bigquery.SchemaField("ong", "STRING"),
            bigquery.SchemaField("arquivoCid", "STRING"),
            bigquery.SchemaField("parceiroContratante", "STRING"),
            bigquery.SchemaField("ongName", "STRING"),
            # Adicione os outros campos do esquema
        ],
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
    )

    with open('/tmp/arquivo_processado.csv', "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)

    job.result()  # Espera a conclusão do job

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

    read_mongo_task = PythonOperator(
        task_id='read_from_mongo',
        python_callable=read_from_mongo,
    )

    upload_bq_task = PythonOperator(
        task_id='upload_to_bigquery',
        python_callable=upload_to_bigquery,
    )

    read_mongo_task >> upload_bq_task