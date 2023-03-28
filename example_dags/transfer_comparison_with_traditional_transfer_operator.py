import os
from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.transfers.s3_to_gcs import S3ToGCSOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator

from universal_transfer_operator.constants import FileType
from universal_transfer_operator.datasets.file.base import File
from universal_transfer_operator.datasets.table import Table
from universal_transfer_operator.universal_transfer_operator import UniversalTransferOperator

s3_bucket = os.getenv("S3_BUCKET", "s3://astro-sdk-test")
gcs_bucket = os.getenv("GCS_BUCKET", "gs://uto-test")
create_table = """
    CREATE OR REPLACE TABLE {{ params.table_name }} (
      sell number,
      list number,
      variable varchar,
      value number);
"""

create_stage = """
    CREATE OR REPLACE STAGE WORKSPACE_STAGE_ONE
    URL='s3://astro-sdk-test/uto/csv_files/homes2.csv'
    FILE_FORMAT=(TYPE=CSV, TRIM_SPACE=TRUE,SKIP_HEADER=1)
    COPY_OPTIONS=(ON_ERROR = CONTINUE)
    storage_integration = aws_int_python_sdk;
    """

drop_table = """
    DROP TABLE IF EXISTS {{ params.table_name }};
"""

with DAG(
    "transfer_comparison_with_tradition_transfer_operator",
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:
    # [START howto_transfer_file_from_s3_to_gcs_using_universal_transfer_operator]
    uto_transfer_non_native_s3_to_gs = UniversalTransferOperator(
        task_id="uto_transfer_non_native_s3_to_gs",
        source_dataset=File(path=f"{s3_bucket}/uto/csv_files/", conn_id="aws_default"),
        destination_dataset=File(
            path=f"{gcs_bucket}/uto/csv_files/",
            conn_id="google_cloud_default",
        ),
    )
    # [END howto_transfer_file_from_s3_to_gcs_using_universal_transfer_operator]

    # [START howto_transfer_file_from_s3_to_gcs_using_traditional_S3ToGCSOperator]
    traditional_s3_to_gcs_transfer = S3ToGCSOperator(
        task_id="traditional_s3_to_gcs_transfer",
        bucket="astro-sdk-test",
        prefix="uto/csv_files/",
        aws_conn_id="aws_default",
        gcp_conn_id="google_cloud_default",
        dest_gcs=f"{gcs_bucket}/uto/csv_files/",
        replace=False,
    )
    # [END howto_transfer_file_from_s3_to_gcs_using_traditional_S3ToGCSOperator]

    # [START howto_transfer_data_from_s3_to_snowflake_using_universal_transfer_operator]
    uto_transfer_non_native_s3_to_snowflake = UniversalTransferOperator(
        task_id="uto_transfer_non_native_s3_to_snowflake",
        source_dataset=File(
            path="s3://astro-sdk-test/uto/csv_files/homes2.csv", conn_id="aws_default", filetype=FileType.CSV
        ),
        destination_dataset=Table(name="uto_s3_table_to_snowflake", conn_id="snowflake_conn"),
    )
    # [END howto_transfer_data_from_s3_to_snowflake_using_universal_transfer_operator]

    # [START howto_transfer_data_from_s3_to_snowflake_using_S3ToSnowflakeOperator]
    snowflake_create_table = SnowflakeOperator(
        task_id="snowflake_create_table",
        sql=create_table,
        params={"table_name": "s3_to_snowflake_table"},
        snowflake_conn_id="snowflake_conn",
    )

    snowflake_create_stage = SnowflakeOperator(
        task_id="snowflake_create_stage", sql=create_stage, snowflake_conn_id="snowflake_conn"
    )

    traditional_copy_from_s3_to_snowflake = S3ToSnowflakeOperator(
        task_id="traditional_copy_from_s3_to_snowflake",
        snowflake_conn_id="snowflake_conn",
        s3_keys="s3://astro-sdk-test/uto/csv_files/homes2.csv",
        table="s3_to_snowflake_table",
        stage="WORKSPACE_STAGE_ONE",
        file_format="(type = 'CSV',field_delimiter = ';')",
    )

    # [END howto_transfer_data_from_s3_to_snowflake_using_S3ToSnowflakeOperator]

    (
        uto_transfer_non_native_s3_to_gs
        >> traditional_s3_to_gcs_transfer
        >> uto_transfer_non_native_s3_to_snowflake
        >> snowflake_create_table
        >> snowflake_create_stage
        >> traditional_copy_from_s3_to_snowflake
    )
