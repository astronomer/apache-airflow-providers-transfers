import os
from datetime import datetime

from airflow import DAG

from universal_transfer_operator.constants import TransferMode
from universal_transfer_operator.datasets.file.base import File
from universal_transfer_operator.datasets.table import Metadata, Table
from universal_transfer_operator.integrations.fivetran.connector import FivetranConnector
from universal_transfer_operator.integrations.fivetran.destination import FivetranDestination
from universal_transfer_operator.integrations.fivetran.fivetran import FiveTranOptions, Group
from universal_transfer_operator.universal_transfer_operator import UniversalTransferOperator

s3_bucket = os.getenv("S3_BUCKET", "s3://astro-sdk-test")
gcs_bucket = os.getenv("GCS_BUCKET", "gs://uto-test")
snowflake_database = os.getenv("SNOWFLAKE_DATABASE", "dummy-database")
snowflake_schema = os.getenv("SNOWFLAKE_SCHEMA", "s3_test")


connector_config = {"is_public": "false", "file_type": "infer", "compression": "infer", "on_error": "fail"}

with DAG(
    "example_universal_transfer_operator_fivetran",
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:
    # [START fivetran_transfer_with_setup]
    transfer_fivetran_with_connector_id = UniversalTransferOperator(
        task_id="transfer_fivetran_with_connector_id",
        source_dataset=File(path=f"{s3_bucket}/uto/", conn_id="aws_default"),
        destination_dataset=Table(name="fivetran_test", conn_id="snowflake_default"),
        transfer_mode=TransferMode.THIRDPARTY,
        transfer_params=FiveTranOptions(conn_id="fivetran_default", connector_id="filing_muppet"),
    )
    # [END fivetran_transfer_with_setup]

    # [START fivetran_transfer_without_setup]
    transfer_fivetran_without_connector_id = UniversalTransferOperator(
        task_id="transfer_fivetran_without_connector_id",
        source_dataset=File(path=f"{s3_bucket}/", conn_id="aws_default", extra={"prefix": "fivetran_test"}),
        destination_dataset=Table(
            name="fivetran_test",
            conn_id="snowflake_conn",
            metadata=Metadata(database=snowflake_database, schema=snowflake_schema),
        ),
        transfer_mode=TransferMode.THIRDPARTY,
        transfer_params=FiveTranOptions(
            conn_id="fivetran_default",
            group=Group(name="test_group_fivetran_snowflake"),
            connector=FivetranConnector(service="s3", config=connector_config, connector_id=None),
            destination=FivetranDestination(
                service="snowflake",
                time_zone_offset="-5",
                region="US",
                config={},
            ),
        ),
    )
    # [END fivetran_transfer_without_setup]
