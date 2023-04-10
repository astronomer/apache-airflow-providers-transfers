from universal_transfer_operator.constants import FileType, TransferMode
from universal_transfer_operator.data_providers.database.snowflake import SnowflakeOptions
from universal_transfer_operator.datasets.file.base import File
from universal_transfer_operator.datasets.table import Table
from universal_transfer_operator.integrations.fivetran import FiveTranOptions
from universal_transfer_operator.universal_transfer_operator import UniversalTransferOperator


def test_option_class_loading_for_dict_to_fivetran_options_class():
    """
    Test that transfer_params dict is converting to fivetran options class
    """
    file_dataset = File(path="s3://astro-sdk/sample.csv", conn_id="aws_default", filetype=FileType.CSV)
    transfer_param = {"conn_id": "fivetran_default"}
    uto = UniversalTransferOperator(
        task_id="s3_to_bigquery",
        source_dataset=file_dataset,
        destination_dataset=file_dataset,
        transfer_mode=TransferMode.THIRDPARTY,
        transfer_params=transfer_param,
    )
    uto._populate_transfer_params()
    assert isinstance(uto.transfer_params, FiveTranOptions)


def test_option_class_loading_for_dict_to_snowflake_options_class():
    """
    Test that transfer_params dict is converting to snowflake options class
    """
    file_dataset = File(path="s3://astro-sdk/sample.csv", conn_id="aws_default", filetype=FileType.CSV)
    table_dataset = Table(conn_id="snowflake_conn")
    uto = UniversalTransferOperator(
        task_id="s3_to_bigquery",
        source_dataset=file_dataset,
        destination_dataset=table_dataset,
        transfer_mode=TransferMode.NATIVE,
        transfer_params={},
    )
    uto._populate_transfer_params()
    assert isinstance(uto.transfer_params, SnowflakeOptions)
