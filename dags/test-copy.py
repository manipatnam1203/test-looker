from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
from airflow import AirflowException


default_args = {
    "start_date": datetime(2023, 6, 28),
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}


dag = DAG(
    "denorm_capa_v1_airflow_dag_test",
    default_args=default_args,
    schedule_interval="*/5 * * * *",
    catchup=False,
    max_active_runs=1
    # Set the schedule as needed
)


# get the current time in epoc
timestamp = int(datetime.utcnow().timestamp())
print(f"Timestamp passed : {timestamp}")

start_job = DummyOperator(task_id="start_job", dag=dag)


def process_bigquery_result(**context):
    # Retrieve the result of the query
    job_id = context["ti"].xcom_pull(task_ids="sync_denormalised_capa_v1_test")
    project_id = "globus-bq-stage"
    location = "US"
    client = bigquery.Client(project=project_id)

    job = client.get_job(job_id, location=location)
    if job.state != "DONE":
        raise Exception(f"The job with ID {job_id} is still running.")

    query_job = client.get_job(job_id)
    query = query_job.query
    key = query.split(",")[-1].rstrip("');").lstrip("'")
    result = client.query(
        f'select * from globus_util.reconcilation where DAG_ID="{key}"'
    )
    CAPA_DENORMALISED = 0
    RAW_JOIN_DATA = 0

    for row in result.result():
        if list(row[0]) == "CAPA_DENORMALISED":
            CAPA_DENORMALISED = list(row)[2]
        if list(row[0]) == "RAW_JOIN_DATA":
            RAW_JOIN_DATA = list(row)[2]

    if CAPA_DENORMALISED == RAW_JOIN_DATA:
        print("DATA VALIDATION SUCCESS")
    else:
        print("DATA VALIDATION FAILED")
        raise AirflowException("DATA VALIDATION FAILED")


trigger_stored_procedure = BigQueryInsertJobOperator(
    task_id="sync_denormalised_capa_v1_test",
    configuration={
        "query": {
            "query": f"CALL `globus-bq-stage.globus_util.DENORMALISED_CAPA_V1_TEST`({timestamp});",
            "useLegacySql": False,
            # "priority": "BATCH",
        }
    },
    dag=dag,
    location="US",  # Set the appropriate BigQuery location
)

reconcile_capa = BigQueryInsertJobOperator(
    task_id="reconcile_denorm_capa_v1_test",
    configuration={
        "query": {
            "query": f"CALL `globus-bq-stage.globus_util.VALIDATE_CAPA_DENORMALISED_V1`({timestamp - 300}, {timestamp},'{{{{ti.dag_id}}}}_{{{{ts_nodash_with_tz}}}}');",
            "useLegacySql": False,
            # "priority": "BATCH",
        }
    },
    dag=dag,
    location="US",  # Set the appropriate BigQuery location
)

end_job = DummyOperator(task_id="end_job", dag=dag)

validate_data = PythonOperator(
    task_id="validate_data",
    python_callable=process_bigquery_result,
    provide_context=True,
    dag=dag,
)


start_job >> trigger_stored_procedure >> reconcile_capa >> validate_data >> end_job
