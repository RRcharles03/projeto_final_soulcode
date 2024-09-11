import pandas as pd
import pymongo
import datetime as dt
import numpy as np
import os
import re

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from pymongo import MongoClient
from google.cloud import bigquery
from dateutil import parser

# Função para converter a data no formato yyyy-mm-dd
def normalizar_data(data):
    try:
        # Converter o valor para datetime e extrair apenas a data
        return pd.to_datetime(data).strftime('%Y-%m-%d')
    except:
        return None  # Se houver erro, retorna None

# Função para padronizar as datas
def padronizar_data(data):
    try:
        # Verificar se o valor é uma string
        if isinstance(data, str):
            # Remover componentes extras como GMT e timezone
            data = data.split(' (')[0]  # Remove a parte da timezone
            parsed_date = parser.parse(data)
            return parsed_date.strftime('%Y-%m-%d')
        else:
            return None  # Se não for string, retorna None
    except (ValueError, TypeError):
        return None  # Retornar None se a data não puder ser parseada


# Função para verificar se o valor é uma data válida no formato dd/mm/aaaa
def validar_data(valor):
    try:
        # Tenta converter o valor em uma data
        return pd.to_datetime(valor, format='%d/%m/%Y', errors='coerce')
    except:
        return np.nan

def extract_transform_mongo(**kwargs):
    try:
        # Conectar ao MongoDB
        client = pymongo.MongoClient('mongodb+srv://teste:a1b2c3@bc26.amljwv1.mongodb.net/?retryWrites=true&w=majority&appName=BC26')

        # # Acessar o banco de dados e a coleção.
        db = client['bases']
        colecao = db['aula1']

        # Ler os dados do MongoDB
        data_Mongo = list(colecao.find())

        # Verifica se não existe uma coleção no mongo, caso positivo retorno uma mensagem
        # caso negativo converte o dicionário em um dataframe
        if not data_Mongo:
            print("Não foi possivel encontrar dados na coleção")
        else:
            df_aluno = pd.DataFrame(data_Mongo)

        # Removendo pontos e hífens do cpf
        df_aluno['cpf'] = df_aluno['cpf'].apply(lambda x: x.replace('.', '').replace('-', '') if isinstance(x, str) else x)

        # Criar um dicionário de substituições
        substituicoes = {
            'BRAS횒LIA': 'Brasília',
            'Sﾃグ PAULO': 'São Paulo',
            'MACEI횙': 'Maceió',
            'GoiÃ¢nia': 'Goiânia',
            'NÃ£o informado': 'Não informado',
            'SÃ£o Paulo': 'São Paulo',
            'SAO PAULO': 'São Paulo',
            'S횄O PAULO': 'São Paulo',
            'SÃ£o CristÃ³vÃ£o': 'São Cristóvão',
            'VitÃ³ria': 'Vitória',
            'Saquarema, RJ': 'Saquarema',
            'DOURADOS -MS': 'Dourados',
            'BelÃ©m': 'Belém',
            'MauÃ¡': 'Mauá',
            'São Bernardo do Campo - SP': 'São Bernardo do Campo',
            'Cachoeira do Campo (Ouro Preto)': 'Ouro Preto',
            'SÃ£o Francisco do Conde': 'São Francisco do Conde',
            'S횄O JO횄O DE MERITI': 'São João de Meriti',
            'EMBU-GUA횉U': 'Embu Guaçu',
            'OlÃ­mpia': 'Olímpia',
            'SÃ£o Bernardo do Campo': 'São Bernardo do Campo',
            'Santo AndrÃ©': 'Santo André',
            'BEL횋M': 'Belém',
            'PERU횒BE': 'Peruíbe',
            'MaringÃ¡': 'Maringá',
            'ParnaÃ­ba': 'Parnaíba',
            'BrasÃ­lia': 'Brasília',
            'S횄O VICENTE': 'São Vicente',
            'UberlÃ¢ndia': 'Uberlândia',
            'Sp': 'São Paulo',
            'SÃ£o JosÃ© do Rio Pardo': 'São José do Rio Pardo',
            'RibeirÃ£o': 'Ribeirão',
            'Poá - Nova Poá': 'Poá',
            'CAMAÇARI, BAHIA': 'Camaçari',
            'EMBU GUA횉U': 'Embu Guaçu',
            'MARIZ횙POLIS': 'Marizópolis',
            'sao paulo': 'São Paulo',
            'TaboÃ£o da Serra': 'Taboão da Serra',
            'Ãguas Lindas de GoiÃ¡s': 'Águas Lindas de Goiás',
            'SimÃµes Filho': 'Simões Filho',
            'Jaboatão dos Guararapes (RMR)': 'Jaboatão dos Guararapes'
        }

        # Aplicar as substituições utilizando o método replace com regex=True
        df_aluno['cidade'] = df_aluno['cidade'].replace(substituicoes, regex=True)
        # Normalizar a capitalização
        df_aluno['cidade'] = df_aluno['cidade'].str.title()

        #Colocar a primeira letra de cada nome em maiúscula e o restante em minúscula
        df_aluno['nomeSocial'] = df_aluno['nomeSocial'].str.title()
        #Substituir valores vazios e NaN por 'não informado'
        df_aluno['nomeSocial'] = df_aluno['nomeSocial'].replace('', np.nan)
        df_aluno['nomeSocial'] = df_aluno['nomeSocial'].fillna('não informado')

        #Merge dos dois campos ceps e drop das colunas que tem suas próprias dags
        df_aluno['cep'] = df_aluno.apply(lambda row: row['cep'] if row['cep'] == row['CEP']
                                    else row['cep'] if pd.notnull(row['cep'])
                                    else row['CEP'], axis=1)
        df_aluno = df_aluno.drop('CEP', axis=1)
        df_aluno = df_aluno.drop('notas', axis=1)
        df_aluno = df_aluno.drop('formacaoAcademicaList', axis=1)
        df_aluno = df_aluno.drop('historico', axis=1)
        df_aluno = df_aluno.drop('empregabilidadeUpdateBy', axis=1)
        df_aluno = df_aluno.drop('_id', axis=1)

        # Padrões de substituição mapeando caracteres errados para os corretos
        padroes_substituicoes = {
            r'Ã§': 'ç',
            r'Ã©': 'é',
            r'Ã¢': 'â',
            r'Ã³': 'ó',
            r'Ã£': 'ã',
            r'Ã': 'í',
            r'Ã¡': 'á',
            r'Ãª': 'ê',
            r'Ã­': 'í',
            r'Ãº': 'ú',
            r'Ã³': 'ó',
            r'ã³': 'ó',
            r'ã©': 'é',
            r'ã§': 'ç',
            r'ã£': 'ã',
            r'ã³': 'ó',
            r'ã³': 'ó',
            r'ã©': 'é',
            r'ã±': 'ñ',
            r'ã´': 'ô',
            r'ãš': 'Ú',
            r'ãœ': 'ü',
            r'íª': 'ê',
            r'í´': 'ô',
            r'í¡': 'á',
            r'íµ': 'õ',
            r'íÍO': 'ção',
            r'Analise': 'Análise',
            r'Eletrica': 'Elétrica',
            r'Gestao': 'Gestão',
            r'Rh': 'Recursos Humanos',
            r'Vet-Ufrrj': 'Medicina Veterinária',
            r'Eng ': 'Engenharia ',
            r'Des. ': 'Desenvolvimento',
            r'Ads': 'Análise e Desenvolvimento de Sistemas',
            r'Medio': 'Médio',
            r'Gestío': 'Gestão'
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

        # Aplicar a função de correção na coluna 'curso'
        df_aluno['areaFormacao'] = df_aluno['areaFormacao'].apply(corrigir_texto)
        # Normalizar a capitalização
        df_aluno['areaFormacao'] = df_aluno['areaFormacao'].str.lower()
        df_aluno['areaFormacao'] = df_aluno['areaFormacao'].str.title()
        # Preenchendo valores nulos como não informados
        df_aluno['areaFormacao'] = df_aluno['areaFormacao'].fillna('Não Informado')

        # Aplicar a função de correção na coluna 'curso'
        df_aluno['graduation'] = df_aluno['graduation'].apply(corrigir_texto)
        # Normalizar a capitalização
        df_aluno['graduation'] = df_aluno['graduation'].str.lower()
        df_aluno['graduation'] = df_aluno['graduation'].str.title()
        # Preenchendo valores nulos como não informados
        df_aluno['graduation'] = df_aluno['graduation'].fillna('Não Informado')

        # Aplicar a função de validação e conversão
        df_aluno['dataNascimento'] = df_aluno['dataNascimento'].apply(lambda x: validar_data(x) if isinstance(x, str) else np.nan)
        # Exibir o DataFrame atualizado
        df_aluno['dataNascimento'] = df_aluno['dataNascimento'].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else np.nan)

        # Aplicar a função no campo 'dataDoBatch'
        df_aluno['dataDoBatch'] = df_aluno['dataDoBatch'].apply(normalizar_data)

        # Aplicar a função ao DataFrame
        df_aluno['empregabilidadeLastUpdate'] = df_aluno['empregabilidadeLastUpdate'].apply(padronizar_data)
        # Convertendo a coluna 'empregabilidadeLastUpdate' para datetime, forçando erros para NaT
        df_aluno['empregabilidadeLastUpdate'] = pd.to_datetime(df_aluno['empregabilidadeLastUpdate'], errors='coerce')
        # Formatando a data para o formato YYYY-MM-DD, e tratando NaT como None
        df_aluno['empregabilidadeLastUpdate'] = df_aluno['empregabilidadeLastUpdate'].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else None)

        #Transformando em um campo numérico e removendo os textos
        df_aluno['mediaFinal'] = pd.to_numeric(df_aluno['mediaFinal'], errors='coerce')
        # Substituir strings vazias e espaços em branco por NaN em todo o DataFrame
        df_aluno = df_aluno.replace(r'^\s*$', np.nan, regex=True)

        #Transformando em um campo numérico e removendo os textos
        df_aluno['mediaGeral'] = pd.to_numeric(df_aluno['mediaGeral'], errors='coerce')
        # Substituir strings vazias e espaços em branco por NaN em todo o DataFrame
        df_aluno = df_aluno.replace(r'^\s*$', np.nan, regex=True)

        df['filhos'] = df['filhos'].astype(str)
        df['favorito'] = df['favorito'].astype(str)
        df['formacaoAcademica'] = df['formacaoAcademica'].astype(str)
        df['selfLearner'] = df['selfLearner'].astype(str)
        df['telefone'] = df['telefone'].astype(str)
        df['parceiroAssociado'] = df['parceiroAssociado'].astype(str)
        df['objetivos'] = df['objetivos'].astype(str)
        df['idiomas'] = df['idiomas'].astype(str)
        df['experienciaProfissional'] = df['experienciaProfissional'].astype(str)
        df['dataDoBatch'] = pd.to_datetime(df['dataDoBatch'], errors='coerce')
        df['dataNascimento'] = pd.to_datetime(df['dataNascimento'], errors='coerce')
        df['empregabilidadeLastUpdate'] = pd.to_datetime(df['empregabilidadeLastUpdate'], errors='coerce')

        # Substituir strings vazias e outros valores vazios por None
        df_aluno.replace('', None, inplace=True)

        # Converter todos os NaN em None
        df_aluno = df_aluno.where(pd.notna(df_aluno), None)

        df['experienciaProfissional'] = df['experienciaProfissional'].apply(lambda x: None if x == [] else x)
        df['objetivos'] = df['objetivos'].apply(lambda x: None if x == [] else x)
        df['idiomas'] = df['idiomas'].apply(lambda x: None if x == [] else x)
        df['objetivos'] = df['objetivos'].apply(lambda x: None if x == [] else x)

        # Salvar o DataFrame como CSV no Bucket do GCS
        tmp_csv_path = '/tmp/arquivo_aluno.csv'
        df_aluno.to_csv(tmp_csv_path, index=False)

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
    table_id = 'arcane-force-428113-v6.soulcodemongo.aluno'

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("numero", "STRING"),
            bigquery.SchemaField("foto", "STRING"),
            bigquery.SchemaField("id", "STRING"),
            bigquery.SchemaField("statusEmpregabilidade", "STRING"),
            bigquery.SchemaField("nacionalidade", "STRING"),
            bigquery.SchemaField("statusNoBootcamp", "STRING"),
            bigquery.SchemaField("cidade", "STRING"),
            bigquery.SchemaField("escolaridadeNivel", "STRING"),
            bigquery.SchemaField("deficiencia", "STRING"),
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
            bigquery.SchemaField("estadoCivil", "STRING"),
            bigquery.SchemaField("objetivos", "STRING"),
            bigquery.SchemaField("nomeSocial", "STRING"),
            bigquery.SchemaField("disponibilidadeMudanca", "STRING"),
            bigquery.SchemaField("telefone", "STRING"),
            bigquery.SchemaField("uf", "STRING"),
            bigquery.SchemaField("bootcampCodigo", "STRING"),
            bigquery.SchemaField("tipoDeficiencia", "STRING"),
            bigquery.SchemaField("dataDoBatch", "DATE"),
            bigquery.SchemaField("areaFormacao", "STRING"),
            bigquery.SchemaField("mediaGeral", "FLOAT"),
            bigquery.SchemaField("bairro", "STRING"),
            bigquery.SchemaField("favorito", "BOOLEAN"),
            bigquery.SchemaField("etnia", "STRING"),
            bigquery.SchemaField("dataNascimento", "DATE"),
            bigquery.SchemaField("idiomas", "STRING"),
            bigquery.SchemaField("formacaoAcademica", "STRING"),
            bigquery.SchemaField("experienciaProfissional", "STRING"),
            bigquery.SchemaField("mediaFinal", "FLOAT"),
            bigquery.SchemaField("email", "STRING"),
            bigquery.SchemaField("cep", "STRING"),
            bigquery.SchemaField("graduation", "STRING"),
            bigquery.SchemaField("statusAuth", "STRING"),
            bigquery.SchemaField("uid", "STRING"),
            bigquery.SchemaField("estudanteId", "STRING"),
            bigquery.SchemaField("selfLearner", "STRING"),
            bigquery.SchemaField("empregabilidadeLastUpdate", "DATE"),
            bigquery.SchemaField("nomeDoCursoSuperior", "STRING"),
            bigquery.SchemaField("cid", "STRING"),
            bigquery.SchemaField("ong", "STRING"),
            bigquery.SchemaField("arquivoCid", "STRING"),
            bigquery.SchemaField("parceiroContratante", "STRING"),
            bigquery.SchemaField("ongName", "STRING"),
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
    'start_date': dt.datetime(2024,9,9),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'dag_mongo_to_bq_aluno',
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