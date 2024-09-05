import pandas as pd
import pymongo
import datetime as dt
import os
import re
import mysql.connector

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from pymongo import MongoClient
from google.cloud import storage

# Configurações da DAG
default_args = {
    'owner': 'projeto_soulcode',
    'depends_on_past': False,
    'start_date': dt.datetime(2024,9,5),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'mongo_to_mysql_pipeline_formacao',
    default_args=default_args,
    description='DAG para extrair do MongoDB, tranformar e enviar para um banco Mysql',
    schedule_interval=dt.timedelta(days=1),
)

# Função para extrair dados do MongoDB
def extract_from_mongo():
    try:
        # Conectar ao MongoDB
        client = pymongo.MongoClient('mongodb+srv://admin:admin1103@projetosoulcode.hjpkc.mongodb.net/?retryWrites=true&w=majority&appName=projetosoulcode')
        
        # Acessar o banco de dados e a coleção
        db = client['projeto_soulcode']
        colecao = db['colecao_fake_formacao']
        
        # Extrai os dados
        data = list(colecao.find())
        client.close()

        # Remover o campo _id de cada documento
        for item in data:
            item.pop('_id', None)  # Usando pop para remover o campo, se existir

        return data

    except Exception as e:
        print(f"Erro ao extrair e tratar os dados: {e}")
        return None

# Função para tratar os dados e transformar em DataFrame
def transform_data(ti):
    data = ti.xcom_pull(task_ids='extract_from_mongo')  # Pega os dados da etapa anterior

    if not data:
        print("Não foi possível encontrar dados na coleção.")
        return
    else:
        df_formacao = pd.DataFrame(data)

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

    # Converter as colunas dataInicio e dataTermino para datetime, mantendo o formato dia/mês/ano
    df_formacao['dataInicio'] = pd.to_datetime(df_formacao['dataInicio'], format='%d/%m/%Y', errors='coerce')
    df_formacao['dataTermino'] = pd.to_datetime(df_formacao['dataTermino'], format='%d/%m/%Y', errors='coerce')

    # Verifica se existe _id proveniente do mongo, caso positivo o mesmo é descartado
    if '_id' in df_formacao.columns:
        df_formacao.drop(columns=['_id'], inplace=True)
    df_formacao.reset_index(drop=True, inplace=True)

    # Salvar o DataFrame como CSV no Bucket do GCS
    tmp_csv_path = '/tmp/arquivo_processado.csv'
    df_formacao.to_csv(tmp_csv_path, index=False)

    # Subir para o Google Cloud Storage
    client = storage.Client()
    bucket = client.get_bucket('seu-bucket-gcs')
    blob = bucket.blob('arquivo_processado.csv')
    blob.upload_from_filename(tmp_csv_path)

    print("Arquivo processado enviado para o GCS.")

    # Armazena o DataFrame em XCom para a próxima etapa
    ti.xcom_push(key='transformed_data', value=df_formacao.to_dict(orient='records'))

# Função para carregar os dados no MySQL
def load_to_mysql(ti):
    # Pega os dados transformados da etapa anterior
    data = ti.xcom_pull(task_ids='transform_data', key='transformed_data')

    # Conecta ao banco de dados MySQL
    cnx = mysql.connector.connect(
        host='34.16.100.83',
        user='root',
        password='',
        database='projetoSoulCode'
    )
    cursor = cnx.cursor()

    # Inserção de dados no MySQL
    insert_query = """
        INSERT INTO formacoes 
        (IdFormacao, status, curso, instituicao, dataInicio, dataTermino, areaFormacao) 
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    
    # Loop para inserir cada registro transformado
    for record in data:
        cursor.execute(insert_query, (
            record['IdFormacao'], 
            record['status'], 
            record['curso'], 
            record['instituicao'], 
            record['dataInicio'], 
            record['dataTermino'], 
            record['areaFormacao']
        ))

    # Confirmar a inserção e fechar conexão
    cnx.commit()
    cursor.close()
    cnx.close()

    print("Dados inseridos no MySQL.")

# Definição das tarefas da DAG
extract_task = PythonOperator(
    task_id='extract_from_mongo',
    python_callable=extract_from_mongo,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_mysql',
    python_callable=load_to_mysql,
    provide_context=True,
    dag=dag,
)

# Definindo a sequência das tarefas
extract_task >> transform_task >> load_task
