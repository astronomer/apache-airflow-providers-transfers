import os
from datetime import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from universal_transfer_operator.constants import FileType
from universal_transfer_operator.datasets.file.base import File
from universal_transfer_operator.datasets.table import Table
from universal_transfer_operator.universal_transfer_operator import UniversalTransferOperator

s3_bucket_path = os.getenv("S3_BUCKET_PATH", "s3://astro-sdk-test/uto/csv_files/")
snowflake_table = os.getenv("SNOWFLAKE_TABLE", "uto_s3_table_to_snowflake")

transform_table = """
    SELECT
       *
    FROM {{ params.table_name }}
"""

with DAG(
    "example_snowflake_transfers",
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:
    # [START howto_transfer_data_from_s3_to_snowflake_using_universal_transfer_operator]
    uto_transfer_non_native_s3_to_snowflake = UniversalTransferOperator(
        task_id="uto_transfer_non_native_s3_to_snowflake",
        source_dataset=File(path=s3_bucket_path, conn_id="aws_default", filetype=FileType.CSV),
        destination_dataset=Table(name="uto_s3_table_to_snowflake", conn_id="snowflake_conn"),
    )
    # [END howto_transfer_data_from_s3_to_snowflake_using_universal_transfer_operator]

    # [START howto_run_sql_table_check]
    snowflake_sql_query = SnowflakeOperator(
        task_id="snowflake_sql_query",
        sql=transform_table,
        params={"table_name": snowflake_table},
        snowflake_conn_id="snowflake_conn",
    )
    # [END howto_run_sql_table_check]

    uto_transfer_non_native_s3_to_snowflake >> snowflake_sql_query
