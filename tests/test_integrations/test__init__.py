import pytest

from universal_transfer_operator.integrations import (
    get_conn_id,
    get_ingestor_option_class,
    get_transfer_integration,
)
from universal_transfer_operator.integrations.fivetran.connector.base import FivetranConnector
from universal_transfer_operator.integrations.fivetran.destination.base import FivetranDestination
from universal_transfer_operator.integrations.fivetran.fivetran import (
    FivetranIntegration,
    FiveTranOptions,
    Group,
)


@pytest.mark.parametrize(
    "transfer",
    [
        {
            "params": FiveTranOptions(
                conn_id="fivetran_default",
                connector_id="filing_muppet",
                group=Group(name="test_group"),
                connector=FivetranConnector(
                    service="s3",
                    config={},
                    connector_id=None,
                    connect_card_config={"connector_val": "test_connector"},
                ),
                destination=FivetranDestination(
                    service="snowflake",
                    time_zone_offset="-5",
                    region="GCP_US_EAST4",
                    config={},
                ),
            ),
            "expected": FivetranIntegration,
        },
    ],
    ids=lambda d: d["params"].conn_id,
)
def test_create_dataprovider(transfer):
    """Test that the correct ingestor class is created for a transfer params"""
    data_provider = get_transfer_integration(transfer_params=transfer["params"])
    assert isinstance(data_provider, transfer["expected"])


@pytest.mark.parametrize(
    "transfer",
    [
        {
            "params": FiveTranOptions(
                conn_id="fivetran_default",
                connector_id="filing_muppet",
                group=Group(name="test_group"),
                connector=FivetranConnector(
                    service="s3",
                    config={},
                    connector_id=None,
                    connect_card_config={"connector_val": "test_connector"},
                ),
                destination=FivetranDestination(
                    service="snowflake",
                    time_zone_offset="-5",
                    region="GCP_US_EAST4",
                    config={},
                ),
            ),
            "expected": FiveTranOptions,
        },
    ],
    ids=lambda d: d["params"].conn_id,
)
def test_get_ingestor_option_class(transfer):
    """Test that the correct ingestor class is created for a transfer params"""
    conn_id = get_conn_id(transfer_params=transfer["params"])
    ingestor_option_class = get_ingestor_option_class(conn_id=conn_id)
    assert isinstance(ingestor_option_class, type(transfer["expected"]))
