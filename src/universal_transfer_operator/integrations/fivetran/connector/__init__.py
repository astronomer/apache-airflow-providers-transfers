from __future__ import annotations

import importlib

from universal_transfer_operator.datasets.base import Dataset
from universal_transfer_operator.integrations.fivetran.connector.base import FivetranConnector
from universal_transfer_operator.utils import get_class_name

CUSTOM_FIVETRAN_CONNECTOR_TYPE_TO_MODULE_PATH = {
    "s3": "universal_transfer_operator.integrations.fivetran.connector.aws.s3",
    "aws": "universal_transfer_operator.integrations.fivetran.connector.aws.s3",
}


def get_fivetran_connector(
    source_dataset: Dataset,
    connector_id: str | None,
    service: str,
    config: dict = {},
    paused: bool = False,
    pause_after_trial: bool = False,
    sync_frequency: str | None = None,
    daily_sync_time: str | None = None,
    schedule_type: str | None = None,
    trust_certificates: bool = False,
    trust_fingerprints: bool = False,
    run_setup_tests: bool = True,
    connect_card_config: dict | None = None,
) -> FivetranConnector:
    """
    Given a source_dataset return the associated Fivetran Connector class.

    :param connector_id: The unique identifier for the connector within the Fivetran system
    :param service: services as per https://fivetran.com/docs/rest-api/connectors#createaconnector
    :param config: Configuration as per destination specified at https://fivetran.com/docs/rest-api/connectors/config
    :param paused: Specifies whether the connector is paused. Defaults to False.
    :param pause_after_trial: Specifies whether the connector should be paused after the free trial period has ended.
     Defaults to False.
    :param sync_frequency: The connector sync frequency in minutes Enum: "5" "15" "30" "60" "120" "180" "360" "480"
     "720" "1440".
    :param daily_sync_time: Defines the sync start time when the sync frequency is already set
    :param schedule_type: Define the schedule type
    :param connect_card_config: Connector card configuration
    :param trust_certificates: Specifies whether we should trust the certificate automatically. The default value is
     FALSE. If a certificate is not trusted automatically,
    :param trust_fingerprints: Specifies whether we should trust the SSH fingerprint automatically. The default value
     is FALSE.
    :param run_setup_tests: Specifies whether the setup tests should be run automatically. The default value is TRUE.
    """
    source_conn_id = source_dataset.conn_id  # type: ignore

    if source_conn_id is None:
        raise ValueError("Connection id for source_dataset is not specified.")

    from airflow.hooks.base import BaseHook

    source_conn_type = BaseHook.get_connection(source_conn_id).conn_type
    module_path = CUSTOM_FIVETRAN_CONNECTOR_TYPE_TO_MODULE_PATH.get(source_conn_type, None)
    if not module_path:
        raise KeyError(f"Connection type {source_conn_type} not supported.")

    module = importlib.import_module(module_path)
    class_name = get_class_name(module_ref=module, suffix="Connector")
    fivetran_connector: FivetranConnector = getattr(module, class_name)(
        connector_id=connector_id,
        service=service,
        config=config,
        paused=paused,
        pause_after_trial=pause_after_trial,
        sync_frequency=sync_frequency,
        daily_sync_time=daily_sync_time,
        schedule_type=schedule_type,
        trust_certificates=trust_certificates,
        trust_fingerprints=trust_fingerprints,
        run_setup_tests=run_setup_tests,
        connect_card_config=connect_card_config,
    )
    return fivetran_connector
