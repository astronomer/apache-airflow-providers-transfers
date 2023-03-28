import os
from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.transfers.s3_to_gcs import S3ToGCSOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator

from universal_transfer_operator.constants import FileType
from universal_transfer_operator.datasets.file.base import File
from universal_transfer_operator.datasets.table import Table
from universal_transfer_operator.universal_transfer_operator import UniversalTransferOperator

s3_bucket = os.getenv("S3_BUCKET", "s3://astro-sdk-test")
gcs_bucket = os.getenv("GCS_BUCKET", "gs://uto-test")


with DAG(
    "transfer_comparison_with_tradition_transfer_operator",
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:
    # transfer file from S3 to GCS using universal transfer operator
    uto_transfer_non_native_s3_to_gs = UniversalTransferOperator(
        task_id="uto_transfer_non_native_s3_to_gs",
        source_dataset=File(path=f"{s3_bucket}/uto/", conn_id="aws_default"),
        destination_dataset=File(
            path=f"{gcs_bucket}/uto/",
            conn_id="google_cloud_default",
        ),
    )

    # transfer files from S3 to GCS using traditional S3ToGCSOperator
    traditional_s3_to_gcs_transfer = S3ToGCSOperator(
        task_id="traditional_s3_to_gcs_transfer",
        bucket="astro-sdk-test",
        prefix="uto",
        aws_conn_id="aws_default",
        gcp_conn_id="google_cloud_default",
        dest_gcs=f"{gcs_bucket}/uto/",
        replace=False,
    )

    # transfer data from S3 to Snowflake using universal transfer operator
    uto_transfer_non_native_s3_to_snowflake = UniversalTransferOperator(
        task_id="uto_transfer_non_native_s3_to_snowflake",
        source_dataset=File(
            path="s3://astro-sdk-test/uto/csv_files/", conn_id="aws_default", filetype=FileType.CSV
        ),
        destination_dataset=Table(name="uto_s3_table_to_snowflake", conn_id="snowflake_conn"),
    )

    # transfer data from S3 to Snowflake using S3ToSnowflakeOperator
    traditional_copy_from_s3_to_snowflake = S3ToSnowflakeOperator(
        task_id="traditional_copy_from_s3_to_snowflake",
        snowflake_conn_id="snowflake_conn",
        s3_keys="s3://astro-sdk-test/uto/csv_files/",
        table="traditional_s3_table_to_snowflake",
        stage="stage_name",
        file_format="(type = 'CSV',field_delimiter = ';')",
        pattern=".*[.]csv",
    )

    (
        uto_transfer_non_native_s3_to_gs
        >> traditional_s3_to_gcs_transfer
        >> uto_transfer_non_native_s3_to_snowflake
        >> traditional_copy_from_s3_to_snowflake
    )
