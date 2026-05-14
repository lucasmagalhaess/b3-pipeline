from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests

EXTRACT_URL = "https://us-central1-b3-pipeline-496319.cloudfunctions.net/extract-b3"
TRANSFORM_URL = "https://us-central1-b3-pipeline-496319.cloudfunctions.net/transform-b3"

default_args = {
    'owner': 'lucas',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'pipeline_b3',
    default_args=default_args,
    description='Pipeline de cotacoes da B3 com camada Silver',
    schedule_interval='0 10,13,17 * * 1-5',
    catchup=False,
    tags=['b3', 'acoes', 'financeiro', 'cloud-functions', 'bigquery'],
) as dag:

    def extract_task(**context):
        print(f"Chamando Cloud Function: {EXTRACT_URL}")
        response = requests.get(EXTRACT_URL, timeout=180)
        response.raise_for_status()
        result = response.json()
        if result.get("status") != "success":
            raise Exception(f"Extracao falhou: {result}")
        filename = result["file"]
        context['task_instance'].xcom_push(key='filename', value=filename)
        print(f"Extraidas: {result['acoes']} acoes")
        return filename

    def transform_task(**context):
        filename = context['task_instance'].xcom_pull(key='filename', task_ids='extrair_cotacoes_b3')
        response = requests.post(TRANSFORM_URL, json={"filename": filename}, timeout=180)
        response.raise_for_status()
        result = response.json()
        if result.get("status") != "success":
            raise Exception(f"Transformacao falhou: {result}")
        print(f"Processadas: {result['acoes_processadas']} acoes")
        return result

    t1 = PythonOperator(task_id='extrair_cotacoes_b3', python_callable=extract_task, provide_context=True)
    t2 = PythonOperator(task_id='transformar_silver_bigquery', python_callable=transform_task, provide_context=True)

    t1 >> t2
