from __future__ import annotations

from abc import abstractmethod
from typing import Any

from universal_transfer_operator.integrations.fivetran.connector import Dataset

airflow_connection_type_to_fivetran_connector_service_mapping = {"s3": "s3", "aws": "s3"}


class FivetranConnector:
    """
    Fivetran connector details. More details at: https://fivetran.com/docs/rest-api/connectors#createaconnector

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

    def __init__(
        self,
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
        connector_id: str | None = None,
        service: str = "",
    ):
        self.connector_id = connector_id
        self.service = service
        self.config = config
        self.paused = paused
        self.pause_after_trial = pause_after_trial
        self.sync_frequency = sync_frequency
        self.daily_sync_time = daily_sync_time
        self.schedule_type = schedule_type
        self.trust_certificates = trust_certificates
        self.trust_fingerprints = trust_fingerprints
        self.run_setup_tests = run_setup_tests
        self.connect_card_config = connect_card_config

    def to_dict(self) -> dict:
        """
        Convert options class to dict
        """
        return vars(self)

    def create_config(
        self,
        source_dataset: Dataset,
        destination_dataset: Dataset,
        group_id: str,
    ) -> Any:
        """
        Creates config based on connection details and config

        :param source_dataset: Source dataset
        :param destination_dataset: Destination dataset
        :param group_id: Group id in fivetran system
        """
        mapped_connection_details = self.map_airflow_connection_to_fivetran(
            source_dataset=source_dataset, destination_dataset=destination_dataset, group_id=group_id
        )
        return self.config | mapped_connection_details  # type: ignore

    @abstractmethod
    def map_airflow_connection_to_fivetran(
        self, source_dataset: Dataset, destination_dataset: Dataset, group_id: str
    ) -> dict[str, str]:
        """
        Maps the airflow connection details to fivetran config for source connector

        :param source_dataset: Source dataset
        :param destination_dataset: Destination dataset
        :param group_id: Group id in fivetran system
        """
        raise NotImplementedError
