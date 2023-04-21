from unittest import mock

import pytest
import requests_mock
from airflow.exceptions import AirflowException

from universal_transfer_operator.datasets.file.base import File
from universal_transfer_operator.datasets.table import Table
from universal_transfer_operator.integrations.fivetran.connector.aws.s3 import S3Connector
from universal_transfer_operator.integrations.fivetran.connector.base import FivetranConnector
from universal_transfer_operator.integrations.fivetran.destination.base import FivetranDestination
from universal_transfer_operator.integrations.fivetran.fivetran import (
    FivetranIntegration,
    FiveTranOptions,
    Group,
)


class TestFivetranIntegration:
    @mock.patch("universal_transfer_operator.integrations.fivetran.fivetran.FivetranIntegration.hook")
    def test_transfer_using_connector_id(self, mock_hook):
        """Test the transfer using connector id."""
        mock_hook.prep_connector.return_value = True
        mock_hook.start_fivetran_sync.return_value = True

        fivetran_integrations = FivetranIntegration()
        assert fivetran_integrations.transfer_using_connector_id(connector_id="dummy") is True

    @mock.patch("universal_transfer_operator.integrations.fivetran.fivetran.FivetranIntegration.create_group")
    @mock.patch(
        "universal_transfer_operator.integrations.fivetran.fivetran.FivetranIntegration.check_group_details"
    )
    def test_create_group_if_needed(self, mock_group, mock_check_group_details):
        """Test to create group using group_id"""
        mock_check_group_details.return_value = False
        mock_group.return_value = "dummy_group"

        fivetran_integrations = FivetranIntegration()
        assert fivetran_integrations.create_group_if_needed(group_id="dummy_group") == "dummy_group"

    @mock.patch("universal_transfer_operator.integrations.fivetran.fivetran.FivetranIntegration.hook")
    def test_check_for_connector_id(self, mock_hook):
        """Test to ensure connector configuration has been completed successfully and is in a functional state."""
        mock_hook.check_connector.return_value = True

        # return False when connector_id is not passed
        fivetran_integrations = FivetranIntegration()
        assert fivetran_integrations.check_for_connector_id() is False

        # return True when connector_id is passed
        fivetran_options = FiveTranOptions(connector_id="dummy_connector_id")
        fivetran_integrations = FivetranIntegration(transfer_params=fivetran_options)
        assert fivetran_integrations.check_for_connector_id() is True

    @mock.patch(
        "universal_transfer_operator.integrations.fivetran.fivetran.FivetranIntegration.check_connector_schema_match"
    )
    def test_fetch_connector_id_from_destination_if_not_exists(self, mock_check_connector_schema_match):
        """Test to fetch the connector_id from destination if it doesn't exist based on group_id and schema."""
        mock_check_connector_schema_match.return_value = False
        MOCK_CONNECTOR_RESPONSE = {
            "code": "Success",
            "data": {
                "items": [
                    {
                        "id": "dummy_connector_id",
                        "schema": "dummy_schema",
                        "status": {"setup_state": "complete"},
                    }
                ]
            },
        }
        with requests_mock.Mocker() as m:
            m.get(
                "https://api.fivetran.com/v1/groups/dummy-id/connectors",
                json=MOCK_CONNECTOR_RESPONSE,
            )

            fivetran_integrations = FivetranIntegration()
            assert (
                fivetran_integrations.fetch_connector_id_from_destination(
                    group_id="dummy-id", destination_dataset=Table(name="dummy", conn_id="dummy")
                )
                is None
            )

    @mock.patch(
        "universal_transfer_operator.integrations.fivetran.fivetran.FivetranIntegration.check_connector_schema_match"
    )
    def test_fetch_connector_id_from_destination_for_success(self, mock_check_connector_schema_match):
        """Test to fetch the connector_id from destination if it exists based on group_id and schema."""
        mock_check_connector_schema_match.return_value = True
        MOCK_CONNECTOR_RESPONSE = {
            "code": "Success",
            "data": {
                "items": [
                    {
                        "id": "dummy_connector_id",
                        "schema": "dummy_schema",
                        "status": {"setup_state": "complete"},
                    }
                ]
            },
        }
        with requests_mock.Mocker() as m:
            m.get(
                "https://api.fivetran.com/v1/groups/dummy-id/connectors",
                json=MOCK_CONNECTOR_RESPONSE,
            )

            fivetran_integrations = FivetranIntegration()
            assert (
                fivetran_integrations.fetch_connector_id_from_destination(
                    group_id="dummy-id", destination_dataset=Table(name="dummy", conn_id="dummy")
                )
                == "dummy_connector_id"
            )

    def test_check_connector_schema_match(self):
        """Test if connector matches the schema of destination."""
        fivetran_integrations = FivetranIntegration()

        assert (
            fivetran_integrations.check_connector_schema_match(
                destination_schema="destination_sch.dummy_table",
                connector_setup_state="connected",
                destination_dataset=Table(name="dummy_table", conn_id="dummy"),
            )
            is True
        )

        assert (
            fivetran_integrations.check_connector_schema_match(
                destination_schema="destination_sch.dummy_table",
                connector_setup_state="connected",
                destination_dataset=Table(name="random_table", conn_id="dummy"),
            )
            is False
        )

    @mock.patch(
        "universal_transfer_operator.integrations.fivetran.fivetran.FivetranIntegration.check_group_name_if_exists"
    )
    def test_check_group_details_already_existing_group_name(self, mock_check_group_name_if_exists):
        """Test to check group details when group_name is already exists."""
        mock_check_group_name_if_exists.return_value = True

        fivetran_integrations = FivetranIntegration()
        assert fivetran_integrations.check_group_details(group_id="dummy_id", group_name="dummy_name") is True

    @mock.patch(
        "universal_transfer_operator.integrations.fivetran.fivetran.FivetranIntegration.check_group_name_if_exists"
    )
    def test_check_group_details_when_group_details_not_exist(self, mock_check_group_name_if_exists):
        """Test to check if group_id  or group_name is already exists."""
        mock_check_group_name_if_exists.return_value = False

        # test success response
        MOCK_GROUP_RESPONSE_SUCCESS = {
            "code": "Success",
            "message": "Group has been created",
            "data": {"id": "dummy_id", "name": "dummy_name", "created_at": "2023-04-21T20:47:21.673879Z"},
        }
        with requests_mock.Mocker() as m:
            m.get(
                "https://api.fivetran.com/v1/groups/dummy_id",
                json=MOCK_GROUP_RESPONSE_SUCCESS,
            )

            fivetran_integrations = FivetranIntegration()
            assert (
                fivetran_integrations.check_group_details(group_id="dummy_id", group_name="dummy_name")
                is True
            )

        # test failure response

        MOCK_GROUP_RESPONSE_FAILURE = {
            "code": "Fail",
            "message": "Group is not created",
        }
        with requests_mock.Mocker() as m:
            m.get(
                "https://api.fivetran.com/v1/groups/dummy_id",
                json=MOCK_GROUP_RESPONSE_FAILURE,
            )

            fivetran_integrations = FivetranIntegration()
            assert (
                fivetran_integrations.check_group_details(group_id="dummy_id", group_name="dummy_name")
                is False
            )

    def test_check_group_name_if_exists(self):
        """Test to check if group_name is exists."""
        MOCK_GROUPS_RESPONSE_SUCCESS = {
            "code": "Success",
            "data": {
                "items": [
                    {"id": "dummy_id", "name": "dummy_name", "created_at": "2023-04-21T20:47:21.673879Z"},
                    {
                        "id": "mill_decibel",
                        "name": "bigquery_test",
                        "created_at": "2023-04-08T21:15:52.285457Z",
                    },
                ]
            },
        }

        with requests_mock.Mocker() as m:
            m.get(
                "https://api.fivetran.com/v1/groups/",
                json=MOCK_GROUPS_RESPONSE_SUCCESS,
            )

            fivetran_integrations = FivetranIntegration()
            assert fivetran_integrations.check_group_name_if_exists(group_name="dummy_name") is True

            assert fivetran_integrations.check_group_name_if_exists(group_name="random_group") is False

    def test_create_group_success(self):
        """Test to check if the group is created based on group name"""
        MOCK_GROUPS_RESPONSE_SUCCESS = {
            "code": "Success",
            "message": "Group has been created",
            "data": {"id": "dummy_id", "name": "dummy_name", "created_at": "2023-04-21T20:47:21.673879Z"},
        }

        with requests_mock.Mocker() as m:
            m.post(
                "https://api.fivetran.com/v1/groups/",
                json=MOCK_GROUPS_RESPONSE_SUCCESS,
            )

            fivetran_options = FiveTranOptions(
                connector_id="dummy_connector_id", group=Group(name="dummy_name")
            )
            fivetran_integrations = FivetranIntegration(transfer_params=fivetran_options)
            assert fivetran_integrations.create_group(group_name="dummy_name") == "dummy_id"

    def test_create_group_failure(self):
        """Test to check if the group creation failed"""
        MOCK_GROUPS_RESPONSE_FAILURE = {
            "code": "Fail",
            "message": "Group creation failed",
        }

        with requests_mock.Mocker() as m:
            m.post(
                "https://api.fivetran.com/v1/groups/",
                json=MOCK_GROUPS_RESPONSE_FAILURE,
            )

            fivetran_options = FiveTranOptions(
                connector_id="dummy_connector_id", group=Group(name="dummy_name")
            )
            fivetran_integrations = FivetranIntegration(transfer_params=fivetran_options)

            with pytest.raises(ValueError, match=r"'code': 'Fail'"):
                fivetran_integrations.create_group(group_name="dummy_name")

    def test_check_destination_details_sucess(self):
        """Test to check if destination_id is exists."""
        MOCK_DESTINATION_RESPONSE_SUCCESS = {
            "code": "Success",
            "data": {
                "id": "housekeeper_mist",
                "group_id": "housekeeper_mist",
                "service": "snowflake",
                "region": "GCP_US_EAST4",
                "time_zone_offset": "-5",
                "setup_status": "connected",
                "config": {},
            },
        }

        with requests_mock.Mocker() as m:
            m.get(
                "https://api.fivetran.com/v1/destinations/housekeeper_mist",
                json=MOCK_DESTINATION_RESPONSE_SUCCESS,
            )

            fivetran_options = FiveTranOptions(
                connector_id="dummy_connector_id", group=Group(name="dummy_name")
            )
            fivetran_integrations = FivetranIntegration(transfer_params=fivetran_options)

            assert fivetran_integrations.check_destination_details(destination_id="housekeeper_mist") is True

    def test_check_destination_details_failure(self):
        """Test to check if destination_id is doesn't exist"""
        # test when destination_id=None
        fivetran_integrations = FivetranIntegration()
        assert fivetran_integrations.check_destination_details(destination_id=None) is False

        MOCK_DESTINATION_RESPONSE_Fail = {
            "code": "Fail",
        }
        with requests_mock.Mocker() as m:
            m.get(
                "https://api.fivetran.com/v1/destinations/housekeeper_mist",
                json=MOCK_DESTINATION_RESPONSE_Fail,
            )

            fivetran_options = FiveTranOptions(
                connector_id="dummy_connector_id", group=Group(name="dummy_name")
            )
            fivetran_integrations = FivetranIntegration(transfer_params=fivetran_options)

            with pytest.raises(ValueError, match=r"'code': 'Fail'"):
                fivetran_integrations.check_destination_details(destination_id="housekeeper_mist")

    def test_fetch_group_id_from_name_for_success(self):
        """Test to fetch group_id from group name"""
        MOCK_GROUPS_RESPONSE_SUCCESS = {
            "code": "Success",
            "data": {
                "items": [
                    {"id": "dummy_id", "name": "dummy_name", "created_at": "2023-04-21T20:47:21.673879Z"},
                    {
                        "id": "mill_decibel",
                        "name": "bigquery_test",
                        "created_at": "2023-04-08T21:15:52.285457Z",
                    },
                ]
            },
        }

        with requests_mock.Mocker() as m:
            m.get(
                "https://api.fivetran.com/v1/groups/",
                json=MOCK_GROUPS_RESPONSE_SUCCESS,
            )

            fivetran_options = FiveTranOptions(
                connector_id="dummy_connector_id", group=Group(name="dummy_name")
            )
            fivetran_integrations = FivetranIntegration(transfer_params=fivetran_options)
            assert fivetran_integrations.fetch_group_id_from_name() == "dummy_id"

    def test_fetch_group_id_from_name_for_failure(self):
        """Test to fetch group_id from group name when error"""
        MOCK_GROUPS_RESPONSE_FAILURE = {"code": "Success", "data": {"items": None}}

        with requests_mock.Mocker() as m:
            m.get(
                "https://api.fivetran.com/v1/groups/",
                json=MOCK_GROUPS_RESPONSE_FAILURE,
            )

            # test for API response error
            fivetran_options = FiveTranOptions(
                connector_id="dummy_connector_id", group=Group(name="dummy_name")
            )
            fivetran_integrations = FivetranIntegration(transfer_params=fivetran_options)
            with pytest.raises(AirflowException, match=r"Group name n"):
                fivetran_integrations.fetch_group_id_from_name()

            # test for group name not found in api response.
            fivetran_options = FiveTranOptions(
                connector_id="dummy_connector_id", group=Group(name="random_name")
            )
            fivetran_integrations = FivetranIntegration(transfer_params=fivetran_options)
            with pytest.raises(AirflowException, match=r"Group name n"):
                fivetran_integrations.fetch_group_id_from_name()

    @mock.patch(
        "universal_transfer_operator.integrations.fivetran.destination.base.FivetranDestination.create_config"
    )
    @mock.patch("airflow.hooks.base.BaseHook")
    @mock.patch(
        "universal_transfer_operator.integrations.fivetran.destination.base.airflow_connection_type_to_fivetran_destination_mapping"
    )
    def test_create_destination_success(
        self, mock_airflow_connection_type_to_fivetran_destination_mapping, mock_base_hook, mock_create_config
    ):
        """Test to check if destination is created based on destination configuration passed"""
        mock_base_hook.get_connection.return_value.conn_type = "snowflake"
        mock_create_config.return_value = {}
        mock_airflow_connection_type_to_fivetran_destination_mapping.return_value = "snowflake"
        MOCK_DESTINATION_RESPONSE_SUCCESS = {
            "code": "Success",
            "message": "Destination has been created",
            "data": {
                "id": "dummy_id",
                "group_id": "dummy_id",
                "service": "snowflake",
                "region": "GCP_US_EAST4",
                "time_zone_offset": "-5",
                "setup_status": "connected",
                "setup_tests": [
                    {"title": "Host Connection", "status": "PASSED", "message": ""},
                    {"title": "Database Connection", "status": "PASSED", "message": ""},
                    {"title": "Permission Test", "status": "PASSED", "message": ""},
                ],
                "config": {
                    "host": "your-account.snowflakecomputing.com",
                    "port": 443,
                    "database": "fivetran",
                    "auth": "PASSWORD",
                    "user": "fivetran_user",
                    "password": "******",
                },
            },
        }

        with requests_mock.Mocker() as m:
            m.post(
                "https://api.fivetran.com/v1/destinations/",
                json=MOCK_DESTINATION_RESPONSE_SUCCESS,
            )

            fivetran_options = FiveTranOptions(
                connector_id="dummy_connector_id",
                group=Group(name="dummy_name"),
                destination=FivetranDestination(config=""),
            )
            fivetran_integrations = FivetranIntegration(transfer_params=fivetran_options)
            assert (
                fivetran_integrations.create_destination(
                    group_id="dummy_id", destination_dataset=Table(name="dummy_name", conn_id="dummy_conn")
                )
                == MOCK_DESTINATION_RESPONSE_SUCCESS
            )

    @mock.patch(
        "universal_transfer_operator.integrations.fivetran.destination.base.FivetranDestination.create_config"
    )
    @mock.patch("airflow.hooks.base.BaseHook")
    @mock.patch(
        "universal_transfer_operator.integrations.fivetran.destination.base.airflow_connection_type_to_fivetran_destination_mapping"
    )
    def test_create_destination_failure(
        self, mock_airflow_connection_type_to_fivetran_destination_mapping, mock_base_hook, mock_create_config
    ):
        """Test to check if destination is failed with correct error"""
        mock_base_hook.get_connection.return_value.conn_type = "snowflake"
        mock_create_config.return_value = {}
        mock_airflow_connection_type_to_fivetran_destination_mapping.return_value = "snowflake"

        # Test when API fails
        MOCK_DESTINATION_RESPONSE_FAILURE = {
            "code": "Fail",
        }

        with requests_mock.Mocker() as m:
            m.post(
                "https://api.fivetran.com/v1/destinations/",
                json=MOCK_DESTINATION_RESPONSE_FAILURE,
            )

            fivetran_options = FiveTranOptions(
                connector_id="dummy_connector_id",
                group=Group(name="dummy_name"),
                destination=FivetranDestination(config=""),
            )
            fivetran_integrations = FivetranIntegration(transfer_params=fivetran_options)
            with pytest.raises(ValueError, match=r"{'code': 'Fail'}"):
                fivetran_integrations.create_destination(
                    group_id="dummy_id", destination_dataset=Table(name="dummy_name", conn_id="dummy_conn")
                )

        # test when API has correct response but destination test fails in fivetran
        MOCK_DESTINATION_RESPONSE_SUCCESS = {
            "code": "Success",
            "message": "Destination has been created",
            "data": {
                "id": "dummy_id",
                "group_id": "dummy_id",
                "service": "snowflake",
                "region": "GCP_US_EAST4",
                "time_zone_offset": "-5",
                "setup_status": "incompleted",
                "setup_tests": [
                    {"title": "Host Connection", "status": "FAILED", "message": ""},
                    {"title": "Database Connection", "status": "FAILED", "message": ""},
                    {"title": "Permission Test", "status": "PASSED", "message": ""},
                ],
                "config": {
                    "host": "your-account.snowflakecomputing.com",
                    "port": 443,
                    "database": "fivetran",
                    "auth": "PASSWORD",
                    "user": "fivetran_user",
                    "password": "******",
                },
            },
        }

        with requests_mock.Mocker() as m:
            m.post(
                "https://api.fivetran.com/v1/destinations/",
                json=MOCK_DESTINATION_RESPONSE_SUCCESS,
            )

            fivetran_options = FiveTranOptions(
                connector_id="dummy_connector_id",
                group=Group(name="dummy_name"),
                destination=FivetranDestination(config=""),
            )
            fivetran_integrations = FivetranIntegration(transfer_params=fivetran_options)
            with pytest.raises(ValueError, match=r"Error occurred"):
                fivetran_integrations.create_destination(
                    group_id="dummy_id", destination_dataset=Table(name="dummy_name", conn_id="dummy_conn")
                )

    @mock.patch(
        "universal_transfer_operator.integrations.fivetran.connector.base.FivetranConnector.create_config"
    )
    @mock.patch("airflow.hooks.base.BaseHook")
    @mock.patch(
        "universal_transfer_operator.integrations.fivetran.connector.base.airflow_connection_type_to_fivetran_connector_service_mapping"
    )
    @mock.patch("universal_transfer_operator.integrations.fivetran.connector.get_fivetran_connector")
    def test_create_connector_success(
        self,
        mock_get_fivetran_connector,
        mock_airflow_connection_type_to_fivetran_connector_service_mapping,
        mock_base_hook,
        mock_create_config,
    ):
        """Test to check if connector is created based on connector configuration passed"""
        mock_get_fivetran_connector.return_value = S3Connector(
            connector_id="dummy_connector_id", service="s3", config={}
        )
        mock_base_hook.get_connection.return_value.conn_type = "s3"
        mock_create_config.return_value = {}
        mock_airflow_connection_type_to_fivetran_connector_service_mapping.return_value = "s3"
        MOCK_CONNECTOR_RESPONSE_SUCCESS = {
            "code": "Success",
            "message": "dummy",
            "data": {
                "id": "dummy_connector_id",
                "service": "s3",
                "schema": "dummy",
                "paused": True,
                "status": {
                    "tasks": [{"code": "dummy", "message": "dummy"}],
                    "warnings": [{"code": "dummy", "message": "dummy"}],
                    "schema_status": "dummy",
                    "update_state": "dummy",
                    "setup_state": "connected",
                    "sync_state": "dummy",
                    "is_historical_sync": True,
                    "rescheduled_for": "2019-08-24T14:15:22Z",
                },
                "daily_sync_time": "dummy",
                "succeeded_at": "2019-08-24T14:15:22Z",
                "connect_card": {"token": "dummy", "uri": "dummy"},
                "sync_frequency": 0,
                "pause_after_trial": True,
                "group_id": "dummy",
                "connected_by": "dummy",
                "setup_tests": [{"title": "dummy", "status": "dummy", "message": "dummy", "details": {}}],
                "source_sync_details": {},
                "service_version": 0,
                "created_at": "2019-08-24T14:15:22Z",
                "failed_at": "2019-08-24T14:15:22Z",
                "schedule_type": "dummy",
                "connect_card_config": {"redirect_uri": "dummy", "hide_setup_guide": True},
            },
        }

        with requests_mock.Mocker() as m:
            m.post(
                "https://api.fivetran.com/v1/connectors/",
                json=MOCK_CONNECTOR_RESPONSE_SUCCESS,
            )

            fivetran_options = FiveTranOptions(
                connector_id="dummy_connector_id", connector=FivetranConnector(config={})
            )
            fivetran_integrations = FivetranIntegration(transfer_params=fivetran_options)
            assert (
                fivetran_integrations.create_connector(
                    group_id="dummy_id",
                    source_dataset=File(path="s3://dummy-bucket/dummy-file", conn_id="dummy_conn"),
                    destination_dataset=Table(name="dummy_name", conn_id="dummy_conn"),
                )
                == "dummy_connector_id"
            )

    @mock.patch(
        "universal_transfer_operator.integrations.fivetran.connector.base.FivetranConnector.create_config"
    )
    @mock.patch("airflow.hooks.base.BaseHook")
    @mock.patch(
        "universal_transfer_operator.integrations.fivetran.connector.base.airflow_connection_type_to_fivetran_connector_service_mapping"
    )
    @mock.patch("universal_transfer_operator.integrations.fivetran.connector.get_fivetran_connector")
    def test_create_connector_failure(
        self,
        mock_get_fivetran_connector,
        mock_airflow_connection_type_to_fivetran_connector_service_mapping,
        mock_base_hook,
        mock_create_config,
    ):
        """Test to check if connector throws error"""
        mock_get_fivetran_connector.return_value = S3Connector(
            connector_id="dummy_connector_id", service="s3", config={}
        )
        mock_base_hook.get_connection.return_value.conn_type = "s3"
        mock_create_config.return_value = {}
        mock_airflow_connection_type_to_fivetran_connector_service_mapping.return_value = "s3"
        MOCK_CONNECTOR_RESPONSE_SUCCESS = {
            "code": "Success",
            "message": "dummy",
            "data": {
                "id": "dummy_connector_id",
                "service": "s3",
                "schema": "dummy",
                "paused": True,
                "status": {
                    "tasks": [{"code": "dummy", "message": "dummy"}],
                    "warnings": [{"code": "dummy", "message": "dummy"}],
                    "schema_status": "dummy",
                    "update_state": "dummy",
                    "setup_state": "failed",
                    "sync_state": "dummy",
                    "is_historical_sync": True,
                    "rescheduled_for": "2019-08-24T14:15:22Z",
                },
                "daily_sync_time": "dummy",
                "succeeded_at": "2019-08-24T14:15:22Z",
                "connect_card": {"token": "dummy", "uri": "dummy"},
                "sync_frequency": 0,
                "pause_after_trial": True,
                "group_id": "dummy",
                "connected_by": "dummy",
                "setup_tests": [{"title": "dummy", "status": "dummy", "message": "dummy", "details": {}}],
                "source_sync_details": {},
                "service_version": 0,
                "created_at": "2019-08-24T14:15:22Z",
                "failed_at": "2019-08-24T14:15:22Z",
                "schedule_type": "dummy",
                "connect_card_config": {"redirect_uri": "dummy", "hide_setup_guide": True},
            },
        }

        with requests_mock.Mocker() as m:
            m.post(
                "https://api.fivetran.com/v1/connectors/",
                json=MOCK_CONNECTOR_RESPONSE_SUCCESS,
            )

            fivetran_options = FiveTranOptions(
                connector_id="dummy_connector_id", connector=FivetranConnector(config={})
            )
            fivetran_integrations = FivetranIntegration(transfer_params=fivetran_options)
            with pytest.raises(ValueError, match=r"Error occurred"):
                fivetran_integrations.create_connector(
                    group_id="dummy_id",
                    source_dataset=File(path="s3://dummy-bucket/dummy-file", conn_id="dummy_conn"),
                    destination_dataset=Table(name="dummy_name", conn_id="dummy_conn"),
                )

    @mock.patch(
        "universal_transfer_operator.integrations.fivetran.fivetran.FivetranIntegration.check_for_connector_id"
    )
    @mock.patch(
        "universal_transfer_operator.integrations.fivetran.fivetran.FivetranIntegration.transfer_using_connector_id"
    )
    def test_transfer_job_when_connector_id_is_passed(
        self, mock_transfer_using_connector_id, mock_check_for_connector_id
    ):
        """
        Test to check loading data from source dataset to the destination using ingestion config when connector_id
        is passed.
        """
        mock_check_for_connector_id.return_value = True
        mock_transfer_using_connector_id.return_value = "success"
        fivetran_options = FiveTranOptions(
            connector_id="dummy_connector_id",
            group=Group(name="dummy_name"),
            destination=FivetranDestination(config=""),
        )
        fivetran_integrations = FivetranIntegration(transfer_params=fivetran_options)
        assert (
            fivetran_integrations.transfer_job(
                source_dataset=File(path="s3://dummy-bucket/dummy-file", conn_id="dummy_conn"),
                destination_dataset=Table(name="dummy_name", conn_id="dummy_conn"),
            )
            == "success"
        )

    @mock.patch(
        "universal_transfer_operator.integrations.fivetran.fivetran.FivetranIntegration.check_for_connector_id"
    )
    @mock.patch(
        "universal_transfer_operator.integrations.fivetran.fivetran.FivetranIntegration.create_group_if_needed"
    )
    @mock.patch(
        "universal_transfer_operator.integrations.fivetran.fivetran.FivetranIntegration.fetch_connector_id_from_destination"
    )
    @mock.patch(
        "universal_transfer_operator.integrations.fivetran.fivetran.FivetranIntegration.transfer_using_connector_id"
    )
    def test_transfer_job_when_connector_id_is_not_passed_with_previous_dag_run_setup(
        self,
        mock_transfer_using_connector_id,
        mock_fetch_connector_id_from_destination,
        mock_create_group_if_needed,
        mock_check_for_connector_id,
    ):
        """
        Test to check loading data from source dataset to the destination using ingestion config when setup is part of
        previous DAG run
        """
        mock_check_for_connector_id.return_value = False
        mock_create_group_if_needed.return_value = "dummy_group"
        mock_fetch_connector_id_from_destination.return_value = "dummy_connector_id"
        mock_transfer_using_connector_id.return_value = "success"

        fivetran_options = FiveTranOptions(
            connector_id="dummy_connector_id",
            group=Group(name="dummy_name"),
        )
        fivetran_integrations = FivetranIntegration(transfer_params=fivetran_options)
        assert (
            fivetran_integrations.transfer_job(
                source_dataset=File(path="s3://dummy-bucket/dummy-file", conn_id="dummy_conn"),
                destination_dataset=Table(name="dummy_name", conn_id="dummy_conn"),
            )
            == "success"
        )

    @mock.patch(
        "universal_transfer_operator.integrations.fivetran.fivetran.FivetranIntegration.check_for_connector_id"
    )
    @mock.patch(
        "universal_transfer_operator.integrations.fivetran.fivetran.FivetranIntegration.create_group_if_needed"
    )
    @mock.patch(
        "universal_transfer_operator.integrations.fivetran.fivetran.FivetranIntegration.fetch_connector_id_from_destination"
    )
    @mock.patch(
        "universal_transfer_operator.integrations.fivetran.fivetran.FivetranIntegration.transfer_using_connector_id"
    )
    @mock.patch(
        "universal_transfer_operator.integrations.fivetran.fivetran.FivetranIntegration.check_destination_details"
    )
    @mock.patch(
        "universal_transfer_operator.integrations.fivetran.fivetran.FivetranIntegration.create_destination"
    )
    @mock.patch(
        "universal_transfer_operator.integrations.fivetran.fivetran.FivetranIntegration.create_connector"
    )
    def test_transfer_job_when_connector_id_is_not_passed(
        self,
        mock_create_connector,
        mock_create_destination,
        mock_check_destination_details,
        mock_transfer_using_connector_id,
        mock_fetch_connector_id_from_destination,
        mock_create_group_if_needed,
        mock_check_for_connector_id,
    ):
        """
        Test to check loading data from source dataset to the destination using ingestion config when setup is not
        done on Fivetran.
        """
        mock_check_for_connector_id.return_value = False
        mock_create_group_if_needed.return_value = "dummy_group"
        mock_fetch_connector_id_from_destination.return_value = None
        mock_transfer_using_connector_id.return_value = "success"
        mock_check_destination_details.return_value = False
        mock_create_destination.return_value = {}
        mock_create_connector.return_value = "dummy_connector_id"

        fivetran_options = FiveTranOptions(
            connector_id="dummy_connector_id",
            group=Group(name="dummy_name"),
        )
        fivetran_integrations = FivetranIntegration(transfer_params=fivetran_options)
        assert (
            fivetran_integrations.transfer_job(
                source_dataset=File(path="s3://dummy-bucket/dummy-file", conn_id="dummy_conn"),
                destination_dataset=Table(name="dummy_name", conn_id="dummy_conn"),
            )
            == "success"
        )
