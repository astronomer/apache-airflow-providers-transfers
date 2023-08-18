import pathlib

import pandas as pd
import pytest
import sqlalchemy
from airflow.hooks.base import BaseHook
from universal_transfer_operator.constants import TransferMode
from universal_transfer_operator.data_providers.database.postgres import PostgresDataProvider
from universal_transfer_operator.datasets.table import Table
from universal_transfer_operator.settings import SCHEMA

CWD = pathlib.Path(__file__).parent

DEFAULT_CONN_ID = "postgres_default"
CUSTOM_CONN_ID = "postgres_conn_pagila"
SUPPORTED_CONN_IDS = [DEFAULT_CONN_ID, CUSTOM_CONN_ID]


@pytest.mark.integration
@pytest.mark.parametrize(
    "conn_id,expected_db_path",
    [
        (
            DEFAULT_CONN_ID,
            BaseHook.get_connection(DEFAULT_CONN_ID).schema,
        ),  # Linux and MacOS have different hosts
        (CUSTOM_CONN_ID, BaseHook.get_connection(CUSTOM_CONN_ID).schema),
    ],
    ids=SUPPORTED_CONN_IDS,
)
def test_postgres_sqlalchemy_engine(conn_id, expected_db_path):
    """Confirm that the SQLAlchemy is created successfully and verify DB path."""
    dp = PostgresDataProvider(
        dataset=Table("some_table", conn_id=conn_id), transfer_mode=TransferMode.NONNATIVE
    )
    engine = dp.sqlalchemy_engine
    assert isinstance(engine, sqlalchemy.engine.base.Engine)
    assert engine.url.database == expected_db_path


@pytest.mark.integration
def test_postgres_run_sql_with_sqlalchemy_text():
    """Run a SQL statement using SQLAlchemy text"""
    statement = sqlalchemy.text("SELECT 1 + 1;")
    dp = PostgresDataProvider(
        dataset=Table("some_table", conn_id="postgres_conn"), transfer_mode=TransferMode.NONNATIVE
    )
    response = dp.run_sql(statement, handler=lambda x: x.first())
    assert response[0] == 2


@pytest.mark.integration
def test_postgres_run_sql():
    """Run a SQL statement using plain string."""
    statement = "SELECT 1 + 1;"
    dp = PostgresDataProvider(
        dataset=Table("some_table", conn_id="postgres_conn"), transfer_mode=TransferMode.NONNATIVE
    )
    response = dp.run_sql(statement, handler=lambda x: x.first())
    assert response[0] == 2


@pytest.mark.integration
def test_postgres_run_sql_with_parameters():
    """Test running a SQL query using SQLAlchemy templating engine"""
    statement = "SELECT 1 + :value;"
    dp = PostgresDataProvider(
        dataset=Table("some_table", conn_id="postgres_conn"), transfer_mode=TransferMode.NONNATIVE
    )
    response = dp.run_sql(statement, parameters={"value": 1}, handler=lambda x: x.first())
    assert response[0] == 2


@pytest.mark.integration
def test_table_exists_raises_exception():
    """Raise an exception when checking for a non-existent table"""
    dp = PostgresDataProvider(
        dataset=Table("some_table", conn_id="postgres_conn"), transfer_mode=TransferMode.NONNATIVE
    )
    assert not dp.table_exists(Table(name="inexistent-table"))


@pytest.mark.integration
@pytest.mark.parametrize(
    "dataset_table_fixture",
    [
        {
            "dataset": "PostgresDataProvider",
            "table": Table(
                columns=[
                    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
                    sqlalchemy.Column("name", sqlalchemy.String(60), nullable=False, key="name"),
                ]
            ),
        },
    ],
    indirect=True,
    ids=["postgres"],
)
def test_postgres_create_table_with_columns(dataset_table_fixture):
    """Create a table using specific columns and types"""
    dp, table = dataset_table_fixture

    statement = f"SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name='{table.name}'"
    response = dp.run_sql(statement, handler=lambda x: x.fetchall())
    assert len(response) == 0

    dp.create_table(table)
    response = dp.run_sql(statement, handler=lambda x: x.fetchall())
    rows = response
    assert len(rows) == 2
    assert rows[0][0:4] == (
        "postgres",
        SCHEMA,
        f"{table.name}",
        "id",
    )
    assert rows[1][0:4] == (
        "postgres",
        SCHEMA,
        f"{table.name}",
        "name",
    )


@pytest.mark.integration
@pytest.mark.parametrize(
    "dataset_table_fixture",
    [
        {
            "dataset": "PostgresDataProvider",
        },
    ],
    indirect=True,
    ids=["postgres"],
)
def test_postgres_create_table_autodetection_without_file(dataset_table_fixture):
    """Create a table using specific columns and types"""
    dp, table = dataset_table_fixture

    statement = f"SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name='{table.name}'"
    response = dp.run_sql(statement, handler=lambda x: x.fetchall())
    assert response == []

    with pytest.raises(ValueError) as exc_info:
        dp.create_table(table)
    assert exc_info.match("File or Dataframe is required for creating table using schema autodetection")


@pytest.mark.integration
@pytest.mark.parametrize(
    "dataset_table_fixture",
    [
        {
            "dataset": "PostgresDataProvider",
        },
    ],
    indirect=True,
    ids=["postgres"],
)
def test_load_pandas_dataframe_to_table(dataset_table_fixture):
    """Load Pandas Dataframe to a SQL table"""
    database, table = dataset_table_fixture

    pandas_dataframe = pd.DataFrame(data={"id": [1, 2]})
    database.load_pandas_dataframe_to_table(pandas_dataframe, table)

    statement = f"SELECT * FROM {database.get_table_qualified_name(table)};"
    response = database.run_sql(statement, handler=lambda x: x.fetchall())

    rows = response
    assert len(rows) == 2
    assert rows[0] == (1,)
    assert rows[1] == (2,)
