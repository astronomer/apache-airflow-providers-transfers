from __future__ import annotations

from abc import abstractmethod
from typing import Any

airflow_connection_type_to_fivetran_destination_mapping = {"snowflake": "snowflake"}


class FivetranDestination:
    """
    Fivetran destination details. More details at: https://fivetran.com/docs/rest-api/destinations/config

    :param service: services as per https://fivetran.com/docs/rest-api/destinations/config
    :param config: Configuration as per destination specified at https://fivetran.com/docs/rest-api/destinations/config
    :param destination_id: The unique identifier for the destination within the Fivetran system
    :param time_zone_offset: Determines the time zone for the Fivetran sync schedule.
    :param region: Data processing location. This is where Fivetran will operate and run computation on data.
    :param run_setup_tests: Specifies whether setup tests should be run automatically.
    """

    def __init__(
        self,
        config: dict = {},
        destination_id: str | None = None,
        time_zone_offset: str | None = "-5",
        region: str | None = "GCP_US_EAST4",
        run_setup_tests: bool | None = True,
        service: str = "",
    ):
        self.service = service
        self.config = config
        self.destination_id = destination_id
        self.time_zone_offset = time_zone_offset
        self.region = region
        self.run_setup_tests = run_setup_tests

    def to_dict(self) -> dict:
        """
        Convert options class to dict
        """
        return vars(self)

    def create_config(
        self, conn_id: str, database_overridden: str | None = None, schema_overridden: str | None = None
    ) -> Any:
        """
        Creates config based on connection details and config

        :param conn_id: Airflow connection id
        :param database_overridden: Database name to be overridden
        :param schema_overridden: Schema name to be overridden
        """
        mapped_connection_details = self.map_airflow_connection_to_fivetran(
            conn_id=conn_id, database_overridden=database_overridden, schema_overridden=schema_overridden
        )
        return self.config | mapped_connection_details  # type: ignore

    @abstractmethod
    def map_airflow_connection_to_fivetran(
        self,
        conn_id: str,
        database_overridden: str | None = None,
        schema_overridden: str | None = None,
    ) -> dict[str, str]:
        """
        Maps the airflow connection details to fivetran config for destination

        :param conn_id: Airflow connection id
        :param database_overridden: Database name to be overridden
        :param schema_overridden: Schema name to be overridden
        """
        raise NotImplementedError
