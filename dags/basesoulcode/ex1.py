from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator, GCSToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.python_operator import PythonOperator

def process_data(**kwargs):
    # Adicione aqui os tratamentos que deseja fazer
    # Exemplo: ler o arquivo, processar os dados, salvar o resultado
    pass

default_args = {
    'owner': 'seu_usuario',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'nome_da_sua_dag',
    default_args=default_args,
    description='Minha DAG para ler e processar arquivos',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Listar arquivos no bucket
    list_files = GCSListObjectsOperator(
        task_id='list_files',
        bucket='nome_do_seu_bucket',
        prefix='caminho/dos/arquivos/',
        google_cloud_storage_conn_id='google_cloud_default',
    )

    # Processar os dados (você pode implementar isso no seu próprio código)
    process_file = PythonOperator(
        task_id='process_file',
        python_callable=process_data,
        provide_context=True,
    )

    # Carregar o arquivo processado para o BigQuery
    load_to_bigquery = GCSToBigQueryOperator(
        task_id='load_to_bigquery',
        bucket='nome_do_seu_bucket',
        source_objects=['caminho/dos/arquivos/arquivo_processado.csv'],
        destination_project_dataset_table='seu_projeto:seu_dataset.sua_tabela',
        schema_fields=[
            {'name': 'coluna1', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'coluna2', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            # Defina seu esquema aqui
        ],
        write_disposition='WRITE_TRUNCATE',
        source_format='CSV',
        skip_leading_rows=1,
        field_delimiter=',',
        google_cloud_storage_conn_id='google_cloud_default',
        bigquery_conn_id='google_cloud_default',
    )

    list_files >> process_file >> load_to_bigquery
