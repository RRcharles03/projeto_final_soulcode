import pandas as pd
import datetime as dt
import uuid
import os
import re
import time

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from pymongo import MongoClient
from google.cloud import bigquery

import pandas as pd
import datetime as dt

# Função para verificar a validade das datas
def verificar_validade_data(data_str):
    if pd.isnull(data_str):
        return None
    try:
        # Verifica se a data está no formato dd/mm/aaaa ou dd/mm/aa
        match = re.match(r'(\d{2})/(\d{2})/(\d{2,4})', str(data_str))
        if match:
            dia, mes, ano = match.groups()
            dia, mes = int(dia), int(mes)
            if len(ano) == 2:
                ano = 2000 + int(ano) if int(ano) < 25 else 1900 + int(ano)
            else:
                ano = int(ano)
            # Verifica se o ano é maior que o ano atual
            if ano > dt.datetime.now().year:
                return None
            # Verifica se o mês está entre 1 e 12
            if mes < 1 or mes > 12:
                return None
            # Verifica se o dia é válido para o mês e ano
            if mes in [4, 6, 9, 11] and (dia < 1 or dia > 30):
                return None
            if mes == 2:
                # Verifica se é ano bissexto
                if (ano % 4 == 0 and ano % 100 != 0) or (ano % 400 == 0):
                    if dia < 1 or dia > 29:
                        return None
                else:
                    if dia < 1 or dia > 28:
                        return None
            elif dia < 1 or dia > 31:
                return None
            # Se tudo estiver correto, retorna a data corrigida
            return f'{dia:02}/{mes:02}/{ano}'
        return None
    except Exception as e:
        print(f"Erro ao verificar a data: {e}")
        return None

# Função para converter a string corrigida em datetime
def converter_para_datetime(df, colunas):
    for coluna in colunas:
        # Aplica a verificação de validade
        df[coluna] = df[coluna].apply(verificar_validade_data)
        # Converte para datetime, onde datas inválidas viram NaT
        df[coluna] = pd.to_datetime(df[coluna], errors='coerce', dayfirst=True)
        # Substitui NaT por None e aplica strftime somente para objetos datetime válidos
        df[coluna] = df[coluna].apply(lambda x: x.strftime('%Y-%m-%d') if isinstance(x, pd.Timestamp) and not pd.isna(x) else None)
    return df

def tratar_data_assinatura(df, coluna):
    # Converte a coluna para datetime, verificando a validade
    df[coluna] = pd.to_datetime(df[coluna], format='%d/%m/%Y %H:%M:%S', errors='coerce')

    # Verifica se há valores inválidos (NaT) e substitui por None
    df[coluna] = df[coluna].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if isinstance(x, pd.Timestamp) and not pd.isna(x) else None)

    return df

def extract_transform_gcs(**kwargs):
    try:
        # URLs dos arquivos CSV no bucket do GCS
        url_csv1 = 'https://storage.googleapis.com/processos-soulcode/df_bruto/processo1.csv'
        url_csv2 = 'https://storage.googleapis.com/processos-soulcode/df_bruto/processo2.csv'

        # Ler os dados dos CSVs diretamente das URLs
        df_csv1 = pd.read_csv(url_csv1, encoding='utf-8')
        df_csv2 = pd.read_csv(url_csv2, encoding='utf-8')
        # Add a coluna Nome da Mãe ao segundo dataframe com campos none, deixando as dois dataframes iguais
        df_csv2['Nome da Mãe'] = None
        # Removendo a primeira coluna dos dataframes
        df_csv1 = df_csv1.drop(df_csv1.columns[0], axis=1)
        df_csv2 = df_csv2.drop(df_csv2.columns[0], axis=1)

        # Concatenar os dois dataframes
        df_processo = pd.concat([df_csv1, df_csv2], ignore_index=True)
        # Remove as linhas que sejam exatamente iguais
        df_processo.drop_duplicates(inplace=True)

        # Salvar o DataFrame como CSV no caminho temporário
        tmp_csv_path = '/tmp/arquivo_processo.csv'
        df_processo.to_csv(tmp_csv_path, encoding='utf-8', index=False)

        # Armazenar o caminho do arquivo no contexto do Airflow para uso na próxima tarefa
        kwargs['ti'].xcom_push(key='/tmp/arquivo_processo.csv', value=tmp_csv_path)

    except Exception as e:
        print(f"Erro ao extrair e tratar os dados: {e}")
        return None

def transform_data(**kwargs):
    tmp_csv_path = kwargs['ti'].xcom_pull(key='/tmp/arquivo_processo.csv', task_ids='extract_transform_gcs')

    if not tmp_csv_path or not os.path.exists(tmp_csv_path):
        raise FileNotFoundError("O arquivo CSV não foi encontrado. Cancelando o envio para o BigQuery.")

    # Carregar o CSV corretamente
    df_processo = pd.read_csv(tmp_csv_path)

    # Renomear colunas
    df_processo.rename(columns={
        'Data de Nascimento': 'dataNascimento',
        'PCD': 'pcd',
        'CID': 'cid',
        'Deficiência': 'deficiencia',
        'Laudo': 'laudo',
        'Gênero': 'genero',
        'Está empregado?': 'estaEmpregado',
        'Orientação Sexual': 'orientacaoSexual',
        'Pronome': 'pronome',
        'Identidade de Gênero': 'identidadeGenero',
        'Estado Civil': 'estadoCivil',
        'Etnia': 'etnia',
        'Órgão Expedidor': 'orgaoExpedidor',
        'Data da Expedição': 'dataExpedicao',
        'Nome da Mãe': 'nomeMae',
        'CEP': 'cep',
        'UF': 'uf',
        'Cidade': 'cidade',
        'Bairro': 'bairro',
        'Logradouro': 'logradouro',
        'Número': 'numero',
        'País': 'pais',
        'ONG': 'ong',
        'Escolaridade': 'escolaridade',
        'Área de Formação': 'areaFormacao',
        'Curso de Formação': 'cursoFormacao',
        'Conhecimento em Inglês': 'conhecimentoIngles',
        'Pergunta Específica (KPMG)': 'perguntaEspecificaKPMG',
        'Data da Inscrição': 'dataInscricao',
        'Etapa': 'etapa',
        'Média Teste Lógico': 'mediaTesteLogico',
        'Resultado Teste Lógico': 'resultadoTesteLogico',
        'Média Teste Técnico': 'mediaTesteTecnico',
        'Resultado Teste Técnico': 'resultadoTesteTecnico',
        'Resultado do Vídeo': 'resultadoVideo',
        'Nota da Dinâmica': 'notaDinamica',
        'Status da Inscrição': 'statusInscricao',
        'Status do Contrato': 'statusContrato',
        'Data Assinatura Distrato': 'dataAssinaturaDistrato',
        'Colaborador Embraer': 'colaboradorEmbraer',
        'Chapa Embraer': 'chapaEmbraer',
        'Liderança Embraer': 'liderancaEmbraer',
        'E-mail Responsável TNT Games': 'emailResponsavelTNTGames',
        'nome2': 'nome2',
        'cpf2': 'cpf2',
        'email2': 'email2'
    }, inplace=True)

    # Substituir valores NaN e 'undefined'
    df_processo = df_processo.where(pd.notnull(df_processo), None)
    df_processo = df_processo.replace('undefined', None)

    # Padronizar campos textuais
    colunas_para_padronizar = ['estaEmpregado', 'cursoFormacao', 'cidade', 'bairro', 'logradouro', 'statusInscricao',  'chapaEmbraer', 'liderancaEmbraer']
    df_processo[colunas_para_padronizar] = df_processo[colunas_para_padronizar].apply(lambda x: x.str.lower().str.title())

    # Ajustar campo CEP
    df_processo['cep'] = df_processo['cep'].apply(lambda x: str(int(x)).zfill(8))

    colunas_datas = ['dataNascimento', 'dataExpedicao', 'dataInscricao']
    # Primeiro verificamos a validade das datas e depois convertemos para o formato datetime
    df_processo = converter_para_datetime(df_processo, colunas_datas)
    df_processo = tratar_data_assinatura(df_processo, 'dataAssinaturaDistrato')

    tmp_csv_path_processo = '/tmp/arquivo_processo.csv'

    df_processo.to_csv(tmp_csv_path_processo, index=False)

    # Armazenar os caminhos dos arquivos no contexto do Airflow para uso na próxima tarefa
    kwargs['ti'].xcom_push(key='tmp_csv_path_processo', value=tmp_csv_path_processo)

def upload_to_bigquery(**kwargs):
    # Recuperar o caminho do arquivo CSV da formcao
    tmp_csv_path_processo = kwargs['ti'].xcom_pull(key='tmp_csv_path_processo', task_ids='transform_data')

    if not tmp_csv_path_processo or not os.path.exists(tmp_csv_path_processo):
        print("O arquivo CSV da processo não foi encontrado. Cancelando o envio para o BigQuery.")
        return

    client = bigquery.Client()
    table_id = 'arcane-force-428113-v6.soulcodeprocesso.processo'

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("dataNascimento", "DATE"),
            bigquery.SchemaField("pcd", "STRING"),
            bigquery.SchemaField("cid", "STRING"),
            bigquery.SchemaField("deficiencia", "STRING"),
            bigquery.SchemaField("laudo", "BOOLEAN"),
            bigquery.SchemaField("genero", "STRING"),
            bigquery.SchemaField("estaEmpregado", "STRING"),
            bigquery.SchemaField("orientacaoSexual", "STRING"),
            bigquery.SchemaField("pronome", "STRING"),
            bigquery.SchemaField("identidadeGenero", "STRING"),
            bigquery.SchemaField("estadoCivil", "STRING"),
            bigquery.SchemaField("etnia", "STRING"),
            bigquery.SchemaField("orgaoExpedidor", "STRING"),
            bigquery.SchemaField("dataExpedicao", "DATE"),
            bigquery.SchemaField("nomeMae", "STRING"),
            bigquery.SchemaField("cep", "STRING"),
            bigquery.SchemaField("uf", "STRING"),
            bigquery.SchemaField("cidade", "STRING"),
            bigquery.SchemaField("bairro", "STRING"),
            bigquery.SchemaField("logradouro", "STRING"),
            bigquery.SchemaField("numero", "STRING"),
            bigquery.SchemaField("pais", "STRING"),
            bigquery.SchemaField("ong", "STRING"),
            bigquery.SchemaField("escolaridade", "STRING"),
            bigquery.SchemaField("areaFormacao", "STRING"),
            bigquery.SchemaField("cursoFormacao", "STRING"),
            bigquery.SchemaField("conhecimentoIngles", "STRING"),
            bigquery.SchemaField("perguntaEspecificaKPMG", "STRING"),
            bigquery.SchemaField("dataInscricao", "DATE"),
            bigquery.SchemaField("etapa", "STRING"),
            bigquery.SchemaField("mediaTesteLogico", "FLOAT"),
            bigquery.SchemaField("resultadoTesteLogico", "STRING"),
            bigquery.SchemaField("mediaTesteTecnico", "FLOAT"),
            bigquery.SchemaField("resultadoTesteTecnico", "STRING"),
            bigquery.SchemaField("resultadoVideo", "STRING"),
            bigquery.SchemaField("notaDinamica", "FLOAT"),
            bigquery.SchemaField("statusInscricao", "STRING"),
            bigquery.SchemaField("statusContrato", "STRING"),
            bigquery.SchemaField("dataAssinaturaDistrato", "DATETIME"),
            bigquery.SchemaField("colaboradorEmbraer", "BOOLEAN"),
            bigquery.SchemaField("chapaEmbraer", "STRING"),
            bigquery.SchemaField("liderancaEmbraer", "STRING"),
            bigquery.SchemaField("emailResponsavelTNTGames", "STRING"),
            bigquery.SchemaField("nome2", "STRING"),
            bigquery.SchemaField("cpf2", "STRING"),
            bigquery.SchemaField("email2", "STRING"),
        ],
        write_disposition="WRITE_TRUNCATE",
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV
    )

    try:
        with open(tmp_csv_path_processo, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_id, job_config=job_config)
        job.result()
        print("processo enviado para o BigQuery com sucesso")
    except Exception as e:
        print(f"Erro ao realizar o upload da processo: {e}")

# Definição da DAG e operadores do Airflow
default_args = {
    'owner': 'projeto_soulcode',
    'depends_on_past': False,
    'start_date': dt.datetime(2024, 9, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'dag_bucket_processo',
    default_args=default_args,
    description='DAG para ler da bucket, processar e enviar ao BigQuery',
    schedule_interval=dt.timedelta(days=1),
    catchup=True,
) as dag:

    extract_transform_gcs = PythonOperator(
        task_id='extract_transform_gcs',
        python_callable=extract_transform_gcs,
    )

    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    upload_bq_task = PythonOperator(
        task_id='upload_to_bigquery',
        python_callable=upload_to_bigquery,
    )



    extract_transform_gcs >> transform_data >> upload_bq_task