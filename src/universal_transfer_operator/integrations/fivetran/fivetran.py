from __future__ import annotations

import json
import logging
import time
import uuid
from functools import cached_property
from typing import Any

import attr
from airflow.exceptions import AirflowException
from attr import field

from universal_transfer_operator.constants import IAM_ROLE_ACTIVATION_WAIT_TIME
from universal_transfer_operator.datasets.base import Dataset
from universal_transfer_operator.integrations.base import TransferIntegration, TransferIntegrationOptions
from universal_transfer_operator.integrations.fivetran.connector import get_fivetran_connector
from universal_transfer_operator.integrations.fivetran.connector.base import (
    FivetranConnector,
    airflow_connection_type_to_fivetran_connector_service_mapping,
)
from universal_transfer_operator.integrations.fivetran.destination import get_fivetran_destination
from universal_transfer_operator.integrations.fivetran.destination.base import (
    FivetranDestination,
    airflow_connection_type_to_fivetran_destination_mapping,
)
from universal_transfer_operator.integrations.hooks.fivetran import FivetranHook


@attr.define
class Group:
    """
    Fivetran group details.

    :param name: The name of the group within Fivetran account.
    :param group_id: Group ID in fivetran system

    """

    name: str
    group_id: str | None = None

    def to_dict(self) -> dict:
        """
        Convert options class to dict
        """
        return attr.asdict(self)


@attr.define
class FivetranOptions(TransferIntegrationOptions):
    """
    FiveTran load options.

    :param conn_id: Connection id of Fivetran
    :param connector_id: The unique identifier for the connector within the Fivetran system
    :param retry_limit: Retry limit. Defaults to 3
    :param retry_delay: Retry delay
    :param poll_interval: Polling interval. Defaults to 15 seconds
    :param schedule_type: Define the schedule type
    :param connector: Connector in FiveTran
    :param group: Group in FiveTran
    :param group_name: Group name in Fivetran system
    :param destination: Destination in Fivetran
    """

    conn_id: str = field(default="fivetran_default")
    connector_id: str | None = field(default="")
    retry_limit: int = 3
    retry_delay: int = 1
    poll_interval: int = 15
    schedule_type: str = "manual"
    connector: FivetranConnector | None = attr.field(default=None)
    group: Group | None = attr.field(default=None)
    group_name: str | None = attr.field(default=None)
    destination: FivetranDestination | None = attr.field(default=None)

    def to_dict(self) -> dict:
        """
        Convert options class to dict
        """
        return attr.asdict(self)


class FivetranIntegration(TransferIntegration):
    """Fivetran integration to transfer datasets using Fivetran APIs."""

    OPTIONS_CLASS = FivetranOptions

    api_user_agent = "airflow_provider_fivetran/1.1.3"
    api_protocol = "https"
    api_host = "api.fivetran.com"
    api_path_connectors = "v1/connectors/"
    api_path_groups = "v1/groups/"
    api_path_destinations = "v1/destinations/"

    def __init__(self, transfer_params: FivetranOptions = FivetranOptions()):
        self.transfer_params: FivetranOptions = transfer_params
        self.transfer_mapping = {}
        super().__init__(transfer_params=self.transfer_params)

    @cached_property
    def hook(self) -> FivetranHook:
        """Return an instance of the database-specific Airflow hook."""
        return FivetranHook(
            self.transfer_params.conn_id,
            retry_limit=self.transfer_params.retry_limit,
            retry_delay=self.transfer_params.retry_delay,
        )

    def set_transfer_params(self):
        """Sets the value from dictionary to FiveTranOptions attributes"""
        if self.transfer_params.group_name:
            self.transfer_params.group = Group(name=self.transfer_params.group_name)
        if self.transfer_params.group and isinstance(self.transfer_params.group, dict):
            self.transfer_params.group = Group(**self.transfer_params.group)
        if self.transfer_params.connector and isinstance(self.transfer_params.connector, dict):
            self.transfer_params.connector = FivetranConnector(**self.transfer_params.connector)
        if self.transfer_params.destination:
            self.transfer_params.destination = FivetranDestination(**self.transfer_params.destination)
        else:
            self.transfer_params.destination = FivetranDestination()

    def transfer_job(self, source_dataset: Dataset, destination_dataset: Dataset) -> Any:
        """
        Loads data from source dataset to the destination using ingestion config

        :param source_dataset: Source dataset
        :param destination_dataset: Destination dataset
        """
        if not source_dataset:
            raise ValueError("Source dataset is not specified.")

        if not destination_dataset:
            raise ValueError("Destination dataset is not specified.")

        # Check if connector_id is passed and check if it exists and do the transfer.
        connector_id = self.transfer_params.connector_id
        if self.check_for_connector_id():
            return self.transfer_using_connector_id(connector_id=connector_id)

        self.set_transfer_params()
        group_id = (
            self.transfer_params.group.group_id
            if self.transfer_params.group and self.transfer_params.group.group_id
            else None
        )

        group_id = self.create_group_if_needed(group_id=group_id)

        # check if destination exists in the group, then fetch connector_id
        connector_id = self.fetch_connector_id_from_destination(
            group_id=group_id, destination_dataset=destination_dataset
        )
        if connector_id:
            return self.transfer_using_connector_id(connector_id=connector_id)

        destination_id = (
            self.transfer_params.destination.destination_id
            if self.transfer_params.destination and self.transfer_params.destination.destination_id
            else None
        )

        if not destination_id or not self.check_destination_details(destination_id=destination_id):
            # Check for destination based on destination_id else create destination
            destination = self.create_destination(group_id=group_id, destination_dataset=destination_dataset)

        logging.info(f"Destination created with destination details: {destination}")
        # Create connector if it doesn't exist
        connector_id = self.create_connector(
            group_id=group_id,
            source_dataset=source_dataset,
            destination_dataset=destination_dataset,
        )

        # Sync connector data
        return self.transfer_using_connector_id(connector_id=connector_id)

    def transfer_using_connector_id(self, connector_id: str | None) -> Any:
        """
        Transfers data from source to destination using connector_id


        :param connector_id: connector_id in fivetran system.
        """
        self.hook.prep_connector(connector_id=connector_id, schedule_type=self.transfer_params.schedule_type)
        # TODO: wait until the job is done
        return self.hook.start_fivetran_sync(connector_id=connector_id)

    def create_group_if_needed(self, group_id: str | None) -> str:
        """
        Create group using group_id

        :param group_id: Group id in fivetran system
        """
        # create a group name if not provided
        group_name = f"universal_transfer_operator_{str(uuid.uuid4())[:8]}"
        if self.transfer_params.group_name:
            group_name = self.transfer_params.group_name

        if not group_id or not self.check_group_details(group_id=group_id, group_name=group_name):
            self.transfer_params.group = Group(name=group_name)
            # create group if not group_id is not passed.
            group_id = self.create_group(group_name=group_name)
            logging.info(f"Group created with group_id: {group_id}")
        return group_id

    def check_for_connector_id(self) -> bool:
        """Ensures connector configuration has been completed successfully and is in a functional state."""
        connector_id = self.transfer_params.connector_id
        if connector_id == "":
            logging.warning("No value specified for connector_id")
            return False
        # Explicitly casting is required since the `self.hook.check_connector` doesn't specify the return type
        return bool(self.hook.check_connector(connector_id=connector_id))

    def fetch_connector_id_from_destination(self, group_id: str, destination_dataset: Dataset) -> Any:
        """
        Fetches the connector_id from destination if it exists based on group_id and schema.

        :param group_id: Group id in fivetran system
        :param destination_dataset: Destination Dataset
        """
        endpoint = self.api_path_groups + group_id + "/connectors"
        api_response = self.hook._do_api_call(("GET", endpoint))
        if api_response["code"] == "Success":
            list_of_connectors = api_response["data"]["items"]
            for individual_connectors in list_of_connectors:
                if self.check_connector_schema_match(
                    destination_schema=individual_connectors["schema"],
                    connector_setup_state=individual_connectors["status"]["setup_state"],
                    destination_dataset=destination_dataset,
                ):
                    logging.info(
                        "connector_id {connector_id} found.",
                        extra={"connector_id": individual_connectors["id"]},
                    )
                    return individual_connectors["id"]
        else:
            logging.warning(
                "No connector found with destination within the group_id: {group_id}",
                extra={"group_id": group_id},
            )
        return None

    @staticmethod
    def check_connector_schema_match(
        destination_schema: str, connector_setup_state: str, destination_dataset: Dataset
    ) -> bool:
        """
        Checks if connector matches the schema of destination.

        :param destination_schema: Destination schema
        :param connector_setup_state: connector setup state
        :param destination_dataset: Destination Dataset
        """
        if (
            connector_setup_state == "connected"
            and destination_schema.split(".")[1] == destination_dataset.name  # type: ignore
        ):
            return True
        return False

    def check_group_details(self, group_id: str, group_name: str) -> bool:
        """
        Check if group_id  or group_name is exists.

        :param group_id: Group id in fivetran system
        :param group_name: Group name in fivetran system
        """

        if group_id is None and group_name is None:
            logging.warning(
                "group_id and group_name are None. It should be the unique identifier for "
                "the group within the Fivetran system. "
            )
            return False

        if self.check_group_name_if_exists(group_name=group_name):
            return True
        # check for group_id if it exists
        endpoint = self.api_path_groups + group_id
        api_response = self.hook._do_api_call(("GET", endpoint))  # skipcq: PYL-W0212
        if api_response["code"] == "Success":
            logging.info("group_id {group_id} found.", extra={"group_id": group_id})
            return True
        else:
            logging.error("group_id {group_id} not found.", extra={"group_id": group_id})

        if group_name is None:
            logging.warning(
                "group_name is None. It should be the identifier for "
                "the group within the Fivetran system. "
            )
            return False
        return False

    def check_group_name_if_exists(self, group_name: str) -> bool:
        """
        Check if group_name is exists.

        :param group_name: Group name in fivetran system
        """
        endpoint = self.api_path_groups
        api_response = self.hook._do_api_call(("GET", endpoint))
        if api_response["code"] == "Success":
            list_of_groups = api_response["data"]["items"]
            for individual_group in list_of_groups:
                if individual_group["name"] == group_name:
                    logging.info("group_name {group_name} found.", extra={"group_name": group_name})
                    return True
        else:
            logging.error("group_name {group_name} not found.", extra={"group_name": group_name})
        return False

    def create_group(self, group_name: str) -> str:
        """
        Creates the group based on group name passed. If name already exists return group name.

        :param group_name: Group name in fivetran system
        """
        endpoint = self.api_path_groups
        group_dict = self.transfer_params.group
        if group_dict is None and group_name is None:
            raise ValueError("Group is none. Pass a valid group")

        group = Group(**group_dict.to_dict())  # type: ignore
        if group.name:
            group_name = group.name
        payload = {"name": group_name}
        try:
            api_response = self.hook._do_api_call(
                ("POST", endpoint), json=json.dumps(payload)
            )  # skipcq: PYL-W0212
            if api_response["code"] == "Success":
                logging.info(api_response)
            else:
                raise ValueError(api_response)
            return str(api_response["data"]["id"])
        except AirflowException as airflow_exec:
            logging.warning(airflow_exec)
        return self.fetch_group_id_from_name()

    def fetch_group_id_from_name(self) -> str:
        """Fetches group name from group id in fivetran."""
        endpoint = self.api_path_groups
        resp = self.hook._do_api_call(("GET", endpoint))
        response = resp["data"]
        group_dict = self.transfer_params.group
        if group_dict is None:
            raise ValueError("Group is none. Pass a valid group")
        group = Group(**group_dict.to_dict())
        if not response.get("items", None):
            raise AirflowException("Group name not found.")

        for item in response["items"]:
            if item["name"] == group.name:
                group_id: str = item["id"]
                logging.debug(f"Group name found with id: {group_id}")
                return group_id
        raise AirflowException("Group name not found.")

    def check_destination_details(self, destination_id: str | None) -> bool:
        """
        Check if destination_id is exists.

        :param destination_id: The unique identifier for the destination within the Fivetran system
        """
        if destination_id is None:
            logging.warning(
                "destination_id is None. It should be the unique identifier for "
                "the destination within the Fivetran system. "
            )
            return False
        endpoint = self.api_path_destinations + destination_id
        api_response = self.hook._do_api_call(("GET", endpoint))  # skipcq: PYL-W0212
        if api_response["code"] == "Success":
            logging.info("destination_id {destination_id} found.", extra={"destination_id": destination_id})
        else:
            raise ValueError(api_response)
        return True

    def create_destination(self, group_id: str, destination_dataset: Dataset) -> dict:
        """
        Creates the destination based on destination configuration passed

        :param group_id: Group id in fivetran system
        :param destination_dataset: Destination dataset
        """
        # create fivetran service from destination dataset
        from airflow.hooks.base import BaseHook

        destination_conn_type = BaseHook.get_connection(destination_dataset.conn_id).conn_type  # type: ignore
        destination_service = airflow_connection_type_to_fivetran_destination_mapping.get(
            destination_conn_type
        )

        endpoint = self.api_path_destinations
        destination_dict = self.transfer_params.destination

        if destination_dict is None:
            logging.info("Destination is passed as none.")
        destination = get_fivetran_destination(
            destination_dataset=destination_dataset,
            service=destination_service,  # type: ignore
            config=self.transfer_params.destination.config,  # type: ignore
            destination_id=self.transfer_params.destination.destination_id,  # type: ignore
            time_zone_offset=self.transfer_params.destination.time_zone_offset,  # type: ignore
            region=self.transfer_params.destination.region,  # type: ignore
            run_setup_tests=self.transfer_params.destination.run_setup_tests,  # type: ignore
        )
        config = destination.create_config(conn_id=destination_dataset.conn_id)  # type: ignore

        payload = {
            "group_id": group_id,
            "service": destination.service,
            "region": destination.region,
            "time_zone_offset": destination.time_zone_offset,
            "config": config,
            "run_setup_tests": destination.run_setup_tests,
        }
        api_response = self.hook._do_api_call(
            ("POST", endpoint), json=json.dumps(payload)
        )  # skipcq: PYL-W0212
        if api_response["code"] == "Success":
            logging.info(api_response)
            setup_status = api_response["data"]["setup_status"]
            if setup_status == "connected":
                logging.info(f"Destination is created with details: {api_response}")
            else:
                api_response_data = api_response["data"]
                logging.error(
                    f"Error occurred in creating the destination with following details {api_response_data}"
                )
                raise ValueError(
                    f"Error occurred in creating the destination with following details {api_response_data}"
                )
        else:
            raise ValueError(api_response)
        return dict(api_response)

    def create_connector(self, group_id: str, source_dataset: Dataset, destination_dataset=Dataset) -> str:
        """
        Creates the connector based on connector configuration passed

        :param group_id: Group id in fivetran system
        :param source_dataset: Source dataset
        :param destination_dataset: Destination dataset
        """

        # create fivetran service from source dataset
        from airflow.hooks.base import BaseHook

        connector_conn_type = BaseHook.get_connection(source_dataset.conn_id).conn_type  # type: ignore
        connector_service = airflow_connection_type_to_fivetran_connector_service_mapping.get(
            connector_conn_type
        )

        endpoint = self.api_path_connectors
        connector_dict = self.transfer_params.connector
        if connector_dict is None:
            logging.info("connector passed is none")
        connector = get_fivetran_connector(
            source_dataset=source_dataset,
            connector_id=self.transfer_params.connector.connector_id,  # type: ignore
            service=connector_service,  # type: ignore
            config=self.transfer_params.connector.config,  # type: ignore
            paused=self.transfer_params.connector.paused,  # type: ignore
            pause_after_trial=self.transfer_params.connector.pause_after_trial,  # type: ignore
            sync_frequency=self.transfer_params.connector.sync_frequency,  # type: ignore
            daily_sync_time=self.transfer_params.connector.daily_sync_time,  # type: ignore
            schedule_type=self.transfer_params.connector.schedule_type,  # type: ignore
            trust_certificates=self.transfer_params.connector.trust_certificates,  # type: ignore
            trust_fingerprints=self.transfer_params.connector.trust_fingerprints,  # type: ignore
            run_setup_tests=self.transfer_params.connector.run_setup_tests,  # type: ignore
            connect_card_config=self.transfer_params.connector.connect_card_config,  # type: ignore
        )  # type: ignore
        config = connector.create_config(
            source_dataset=source_dataset, destination_dataset=destination_dataset, group_id=group_id
        )

        payload = {
            "group_id": group_id,
            "service": connector_service,
            "trust_certificates": connector.trust_certificates,
            "trust_fingerprints": connector.trust_fingerprints,
            "run_setup_tests": connector.run_setup_tests,
            "paused": connector.paused,
            "pause_after_trial": connector.pause_after_trial,
            "sync_frequency": connector.sync_frequency,
            "daily_sync_time": connector.daily_sync_time,
            "schedule_type": connector.schedule_type,
            "config": config,
        }

        # wait for 60 seconds for IAM roles to be effective
        time.sleep(IAM_ROLE_ACTIVATION_WAIT_TIME)

        api_response = self.hook._do_api_call(
            ("POST", endpoint), json=json.dumps(payload)
        )  # skipcq: PYL-W0212
        if api_response["code"] == "Success":
            logging.info(api_response)
            setup_state = api_response["data"]["status"]
            if setup_state["setup_state"] == "connected":
                logging.info(f"Connector is created with details: {api_response}")
            else:
                api_response_data = api_response["data"]
                logging.error(
                    f"Error occurred in creating the connector with following details {api_response_data}"
                )
                raise ValueError(
                    f"Error occurred in creating the connector with following details {api_response_data}"
                )
        else:
            raise ValueError(api_response)
        return str(api_response["data"]["id"])
