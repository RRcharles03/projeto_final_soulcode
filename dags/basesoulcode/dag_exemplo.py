from datetime import datetime, timedelta
from airflow import DAG

from airfflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

default_args = {
    "owner": "",
    "depends_on_past": False,
    "email": [""],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    'schedule_interval': '@daily'
    "start_date": YESTERDAY,
}

with DAG(
  dag_id = 'move_gcs_files',
  default_args = default_args,
  schedule_interval = None,
  catchup= False,
  tags = ['gcs', 'youtube']
) as dag: 
  
    moves_files_to_bkp = GCSTpTGCSOperator(
        task_id='move_files_to_bkp', # nome do arquivo
        source_bucket='staging-youtube', # nome da bucket de origem
        source_object="*",  # objetos de origem
        destination_bucket = "", # bucket de destino
        destination_object = "",
        move_object = True
    )

    moves_files_to_bkp