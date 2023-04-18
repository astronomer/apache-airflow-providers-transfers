from __future__ import annotations

import importlib

from universal_transfer_operator.datasets.base import Dataset
from universal_transfer_operator.integrations.fivetran.destination.base import FivetranDestination
from universal_transfer_operator.utils import get_class_name

CUSTOM_FIVETRAN_DESTINATION_TYPE_TO_MODULE_PATH = {
    "snowflake": "universal_transfer_operator.integrations.fivetran.destination.snowflake"
}


def get_fivetran_destination(
    destination_dataset: Dataset,
    service: str,
    config: dict = {},
    destination_id: str | None = None,
    time_zone_offset: str | None = "-5",
    region: str | None = "US",
    run_setup_tests: bool | None = True,
) -> FivetranDestination:
    """
    Given a destination_dataset return the associated Fivetran Destination class.

    :param destination_dataset: Destination dataset
    :param service: services as per https://fivetran.com/docs/rest-api/destinations/config
    :param config: Configuration as per destination specified at https://fivetran.com/docs/rest-api/destinations/config
    :param destination_id: The unique identifier for the destination within the Fivetran system
    :param time_zone_offset: Determines the time zone for the Fivetran sync schedule.
    :param region: Data processing location. This is where Fivetran will operate and run computation on data.
    :param run_setup_tests: Specifies whether setup tests should be run automatically.
    """
    destination_conn_id = destination_dataset.conn_id  # type: ignore

    if destination_conn_id is None:
        raise ValueError("Connection id for destination_dataset is not specified.")

    from airflow.hooks.base import BaseHook

    destination_conn_type = BaseHook.get_connection(destination_conn_id).conn_type
    module_path = CUSTOM_FIVETRAN_DESTINATION_TYPE_TO_MODULE_PATH.get(destination_conn_type, None)
    if not module_path:
        raise KeyError(f"Connection type {destination_conn_type} not supported.")

    module = importlib.import_module(module_path)
    class_name = get_class_name(module_ref=module, suffix="Destination")
    fivetran_destination: FivetranDestination = getattr(module, class_name)(
        service=service,
        config=config,
        destination_id=destination_id,
        time_zone_offset=time_zone_offset,
        region=region,
        run_setup_tests=run_setup_tests,
    )
    return fivetran_destination
