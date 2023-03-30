import os
import pathlib
from datetime import datetime

from airflow import DAG

from universal_transfer_operator.constants import FileType
from universal_transfer_operator.datasets.file.base import File
from universal_transfer_operator.datasets.table import Table
from universal_transfer_operator.universal_transfer_operator import (
    TransferIntegrationOptions,
    UniversalTransferOperator,
)

CWD = pathlib.Path(__file__).parent
DATA_DIR = str(CWD) + "/../../data/"
s3_bucket = os.getenv("S3_BUCKET", "s3://astro-sdk-test")
gcs_bucket = os.getenv("GCS_BUCKET", "gs://uto-test")

with DAG(
    "example_sqlite_transfers_append_and_replace_strategies",
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:
    # [START howto_transfer_data_from_s3_to_sqlite_using_replace]
    uto_transfer_non_native_s3_to_sqlite_replace = UniversalTransferOperator(
        task_id="uto_transfer_non_native_s3_to_sqlite_replace",
        source_dataset=File(path=f"{DATA_DIR}sample.csv", filetype=FileType.CSV),
        destination_dataset=Table(name="uto_s3_table_to_sqlite_replace", conn_id="sqlite_default"),
    )
    # [END howto_transfer_data_from_s3_to_sqlite_using_replace]

    # [START howto_transfer_data_from_s3_to_sqlite_using_append]
    uto_transfer_non_native_s3_to_sqlite_append = UniversalTransferOperator(
        task_id="uto_transfer_non_native_s3_to_sqlite_append",
        source_dataset=File(path=f"{DATA_DIR}sample.csv", filetype=FileType.CSV),
        destination_dataset=Table(name="uto_s3_table_to_sqlite_append", conn_id="snowflake_conn"),
        transfer_params=TransferIntegrationOptions(if_exists="append"),
    )
    # [END howto_transfer_data_from_s3_to_sqlite_using_append]
