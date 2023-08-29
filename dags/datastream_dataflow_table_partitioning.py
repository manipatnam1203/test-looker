from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta
from airflow.exceptions import AirflowFailException
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 7, 3),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG("datastream_dataflow_table_partitioning", default_args=default_args, schedule_interval=None)

dataflow_template_path = (
    "gs://dataflow-templates-us-central1/latest/flex/Cloud_Datastream_to_BigQuery"
)
project_id = "globus-bq-stage"
region = "us-central1"
job_name = "datastreamtobq"
gcs_path = "gs://globus_datastream/poc"
pubsub_subscription = "projects/globus-bq-stage/subscriptions/datastream-subscription"
bigquery_dataset = "globus_dscsbq"
dead_letter_queue = "gs://globus_datastream/dead_queue"
input_file_format = "avro"
dataflow_command = f'gcloud beta dataflow flex-template run datastream-replication \
        --project="{project_id}" --region="{region}" \
        --template-file-gcs-location="{dataflow_template_path}" \
        --enable-streaming-engine \
        --parameters \
inputFilePattern="{gcs_path}",\
gcsPubSubSubscription="{pubsub_subscription}",\
outputStagingDatasetTemplate="{bigquery_dataset}",\
outputDatasetTemplate="{bigquery_dataset}",\
deadLetterQueueDirectory="{dead_letter_queue}",\
inputFileFormat="{input_file_format}"'

list_jobs_command = f"gcloud dataflow jobs list --filter='name={job_name}' --status=active --region={region}"

existing_table_name = "globus-bq-stage.globus_dscsbq.BQ_STRESS_TEST2"
temporary_table_name = existing_table_name + "_temp"
partition_type = "date"
partition_column = "C501_ORDER_DATE"


# dummy operators to mark start and the end time 
start_task = DummyOperator(task_id="start_task", dag=dag)

end_task = DummyOperator(task_id="end_task", dag=dag)

def get_dataflow_job_id(project_id, **context):
    ti = context["ti"]
    job_details = ti.xcom_pull(task_ids="list_jobs")
    job_id = ""
    if job_details is not None or job_details == "Listed 0 items.":
        job_id = job_details.split(" ")[0]
        ti.xcom_push(key="job-id", value=job_id)
        return job_id
    else:
        raise AirflowFailException("Invalid Job ID")


list_dataflow_jobs = BashOperator(
    task_id="list_dataflow_jobs",
    bash_command=list_jobs_command,
    do_xcom_push=True,
    # xcom_push=True,
    dag=dag,
)


get_datastream_dataflow_job_id = PythonOperator(
    task_id="get_datastream_dataflow_job_id",
    python_callable=get_dataflow_job_id,
    op_kwargs={"project_id": project_id},
    provide_context=True,
    dag=dag,
)

drain_datasteam_dataflow_job = BashOperator(
    task_id="drain_datasteam_dataflow_job",
    bash_command="gcloud dataflow jobs drain  $VAR1 --region=$VAR2",
    env={"VAR1": '{{ ti.xcom_pull(key="job-id")}}', "VAR2": region},
    do_xcom_push=True,
    # xcom_push=True,
    dag=dag,
)

create_new_temp_table = BigQueryInsertJobOperator(
    task_id="create_new_temp_table",
    configuration={
        "query": {
            "query": f"create table `{temporary_table_name}` partition by {partition_type}({partition_column}) as SELECT * FROM `{existing_table_name}`",
            "useLegacySql": False,
            # "priority": "BATCH",
        }
    },
    dag=dag,
    location="US",  # Set the appropriate BigQuery location
)

drop_existing_table = BigQueryInsertJobOperator(
    task_id="drop_existing_table",
    configuration={
        "query": {
            "query": f"drop table `{existing_table_name}`",
            "useLegacySql": False,
            # "priority": "BATCH",
        }
    },
    dag=dag,
    location="US",  # Set the appropriate BigQuery location
)

rename_temp_table = BigQueryInsertJobOperator(
    task_id="rename_temp_table",
    configuration={
        "query": {
            "query": f"alter table `{temporary_table_name}` rename to `{existing_table_name.split('.')[-1]}`",
            "useLegacySql": False,
            # "priority": "BATCH",
        }
    },
    dag=dag,
    location="US",  # Set the appropriate BigQuery location
)

start_dataflow_job = BashOperator(
    task_id="start_dataflow_job", bash_command=dataflow_command, dag=dag
)


start_task>>list_dataflow_jobs>> get_datastream_dataflow_job_id>> drain_datasteam_dataflow_job>> create_new_temp_table>> drop_existing_table>> rename_temp_table>> start_dataflow_job>>end_task


# dataflow_task
