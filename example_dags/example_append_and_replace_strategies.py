import os
from datetime import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from universal_transfer_operator.constants import FileType
from universal_transfer_operator.datasets.file.base import File
from universal_transfer_operator.datasets.table import Table
from universal_transfer_operator.universal_transfer_operator import (
    TransferIntegrationOptions,
    UniversalTransferOperator,
)

s3_bucket = os.getenv("S3_BUCKET", "s3://astro-sdk-test")
gcs_bucket = os.getenv("GCS_BUCKET", "gs://uto-test")
transform_table = """
    SELECT
       *
    FROM {{ params.table_name }}
"""

with DAG(
    "example_snowflake_transfers_append_and_replace_strategies",
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:
    # [START howto_transfer_data_from_s3_to_snowflake_using_replace]
    uto_transfer_non_native_s3_to_snowflake_replace = UniversalTransferOperator(
        task_id="uto_transfer_non_native_s3_to_snowflake_replace",
        source_dataset=File(
            path="s3://astro-sdk-test/uto/csv_files/", conn_id="aws_default", filetype=FileType.CSV
        ),
        destination_dataset=Table(name="uto_s3_table_to_snowflake_replace", conn_id="snowflake_conn"),
    )
    # [END howto_transfer_data_from_s3_to_snowflake_using_replace]

    # [START howto_run_sql_table_check_after_replace]
    snowflake_sql_query_after_replace = SnowflakeOperator(
        task_id="snowflake_sql_query_after_replace",
        sql=transform_table,
        params={"table_name": "uto_s3_table_to_snowflake_replace"},
        snowflake_conn_id="snowflake_conn",
    )
    # [END howto_run_sql_table_check_after_replace]

    # [START howto_transfer_data_from_s3_to_snowflake_using_append]
    uto_transfer_non_native_s3_to_snowflake_append = UniversalTransferOperator(
        task_id="uto_transfer_non_native_s3_to_snowflake_append",
        source_dataset=File(
            path="s3://astro-sdk-test/uto/csv_files/home2.csv", conn_id="aws_default", filetype=FileType.CSV
        ),
        destination_dataset=Table(name="uto_s3_table_to_snowflake_append", conn_id="snowflake_conn"),
        transfer_params=TransferIntegrationOptions(if_exists="append"),
    )
    # [END howto_transfer_data_from_s3_to_snowflake_using_append]

    # [START howto_run_sql_table_check_after_append]
    snowflake_sql_query_after_append = SnowflakeOperator(
        task_id="snowflake_sql_query_after_append",
        sql=transform_table,
        params={"table_name": "uto_s3_table_to_snowflake_append"},
        snowflake_conn_id="snowflake_conn",
    )
    # [END howto_run_sql_table_check_after_append]

    uto_transfer_non_native_s3_to_snowflake_replace >> snowflake_sql_query_after_replace
    uto_transfer_non_native_s3_to_snowflake_append >> snowflake_sql_query_after_append
