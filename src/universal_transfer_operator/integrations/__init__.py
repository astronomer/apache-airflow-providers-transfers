from __future__ import annotations

import importlib
from typing import Type, cast

from airflow.hooks.base import BaseHook

from universal_transfer_operator.constants import IngestorSupported
from universal_transfer_operator.integrations.base import TransferIntegration, TransferIntegrationOptions
from universal_transfer_operator.utils import get_class_name

CUSTOM_INGESTION_TYPE_TO_MODULE_PATH = {
    "fivetran": "universal_transfer_operator.integrations.fivetran.fivetran"
}


def get_transfer_integration(transfer_params: TransferIntegrationOptions) -> TransferIntegration:
    """
    Given a transfer_params return the associated TransferIntegrations class instance.

    :param transfer_params: kwargs to be used by methods involved in transfer using FiveTran.
    """
    conn_id = get_conn_id(transfer_params=transfer_params)
    return get_ingestor_class(conn_id=conn_id)(transfer_params)


def get_ingestor_class(conn_id: str) -> type[TransferIntegration]:
    """
    Given a transfer_params return the associated TransferIntegrations class.

    :param transfer_params: kwargs to be used by methods involved in transfer using FiveTran.
    """
    thirdparty_conn_type = BaseHook.get_connection(conn_id).conn_type
    if thirdparty_conn_type not in {item.value for item in IngestorSupported}:
        raise ValueError("Ingestion platform not yet supported.")

    module_path = CUSTOM_INGESTION_TYPE_TO_MODULE_PATH[thirdparty_conn_type]
    module = importlib.import_module(module_path)
    class_name = get_class_name(module_ref=module, suffix="Integration")
    return cast(Type[TransferIntegration], getattr(module, class_name))


def get_ingestor_option_class(conn_id: str) -> type[TransferIntegrationOptions]:
    """
    Get options class for fivetran
    """
    class_ref = get_ingestor_class(conn_id)
    return getattr(class_ref, "OPTIONS_CLASS", TransferIntegrationOptions)


def get_conn_id(transfer_params: TransferIntegrationOptions | dict) -> str:
    """
    Get conn_id from transfer_params
    """
    try:
        conn_id = transfer_params.conn_id  # type: ignore
    except AttributeError:
        conn_id = transfer_params.get("conn_id")  # type: ignore
    if conn_id is None:
        raise ValueError("Connection id for integration is not specified.")
    return conn_id
