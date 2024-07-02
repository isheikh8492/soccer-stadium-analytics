from airflow import DAG
from airflow.operators.python import PythonOperator
from pipelines.wikipedia_pipeline import extract_data_from_wikipedia, transform_wikipedia_data, write_wikipedia_data
from datetime import datetime

dag = DAG(dag_id="wikipedia_flow", schedule_interval="@daily",
          default_args={"owner": "airflow", "start_date": datetime(2024, 6, 28)},
          description="A simple DAG to scrape Wikipedia",
          catchup=False)

## Extraction
extract_wikipedia_data = PythonOperator(
    task_id="extract_data_from_wikipedia",
    python_callable=extract_data_from_wikipedia,
    provide_context=True,
    op_kwargs={
        "url": "https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity"
    },
    dag=dag,
)

## Preprocessing
transform_data_from_wikipedia = PythonOperator(
    task_id="transform_wikipedia_data",
    python_callable=transform_wikipedia_data,
    provide_context=True,
    dag=dag,
)

## Write to DB
write_data_from_wikipedia = PythonOperator(
    task_id="write_wikipedia_data",
    python_callable=write_wikipedia_data,
    provide_context=True,
    dag=dag,
)

extract_wikipedia_data >> transform_data_from_wikipedia >> write_data_from_wikipedia
