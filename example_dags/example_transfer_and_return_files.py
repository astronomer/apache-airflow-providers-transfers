import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator

from universal_transfer_operator.datasets.file.base import File
from universal_transfer_operator.universal_transfer_operator import UniversalTransferOperator

s3_bucket = os.getenv("S3_BUCKET", "s3://astro-sdk-test")
gcs_bucket = os.getenv("GCS_BUCKET", "gs://uto-test")


def calculate_the_file_transferred(ti):
    """Return the number of files transferred."""
    transferred_files = ti.xcom_pull(key="return_value", task_ids=["transfer_non_native_s3_to_gs"])
    listed_file_in_s3 = ti.xcom_pull(key="return_value", task_ids=["list_s3_files"])
    assert len(transferred_files[0]) == len(listed_file_in_s3[0])


with DAG(
    "example_transfers_and_return_files",
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:
    transfer_non_native_s3_to_gs = UniversalTransferOperator(
        task_id="transfer_non_native_s3_to_gs",
        source_dataset=File(path=f"{s3_bucket}/uto/", conn_id="aws_default"),
        destination_dataset=File(
            path=f"{gcs_bucket}/uto/",
            conn_id="google_cloud_default",
        ),
    )

    list_s3_files = S3ListOperator(
        task_id="list_s3_files", bucket="astro-sdk-test", prefix="uto/", aws_conn_id="aws_default"
    )

    files_transferred = PythonOperator(
        task_id="files_transferred", python_callable=calculate_the_file_transferred
    )

    transfer_non_native_s3_to_gs >> list_s3_files >> files_transferred
