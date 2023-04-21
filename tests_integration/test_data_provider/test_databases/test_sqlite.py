import pathlib

import pytest
import sqlalchemy

from universal_transfer_operator.datasets.file.base import File
from universal_transfer_operator.datasets.table import Table

CWD = pathlib.Path(__file__).parent.parent


@pytest.mark.integration
@pytest.mark.parametrize(
    "src_dataset_fixture",
    [
        {
            "name": "SqliteDataProvider",
            "object": Table(
                columns=[
                    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
                    sqlalchemy.Column("name", sqlalchemy.String(60), nullable=False, key="name"),
                ]
            ),
        }
    ],
    indirect=True,
    ids=["sqlite"],
)
def test_sqlite_create_table_with_columns(src_dataset_fixture):
    """Create a table using specific columns and types"""
    dp, dataset_object = src_dataset_fixture

    statement = f"PRAGMA table_info({dataset_object.name});"
    response = dp.run_sql(statement, handler=lambda x: x.first())
    assert response is None

    dp.create_table(dataset_object)
    response = dp.run_sql(statement, handler=lambda x: x.fetchall())
    rows = response
    assert len(rows) == 2
    assert rows[0] == (0, "id", "INTEGER", 1, None, 1)
    assert rows[1] == (1, "name", "VARCHAR(60)", 1, None, 0)


@pytest.mark.integration
@pytest.mark.parametrize(
    "src_dataset_fixture",
    [
        {
            "name": "SqliteDataProvider",
        }
    ],
    indirect=True,
    ids=["sqlite"],
)
def test_sqlite_create_table_autodetection_with_file(src_dataset_fixture):
    """Create a table using file"""
    dp, dataset_object = src_dataset_fixture

    statement = f"PRAGMA table_info({dataset_object.name});"
    response = dp.run_sql(statement, handler=lambda x: x.first())
    assert response is None

    filepath = str(pathlib.Path(CWD.parent, "data/sample.csv"))
    dp.create_table(dataset_object, File(filepath))
    response = dp.run_sql(statement, handler=lambda x: x.fetchall())
    rows = response
    assert len(rows) == 2
    assert rows[0] == (0, "id", "BIGINT", 0, None, 0)
    assert rows[1] == (1, "name", "TEXT", 0, None, 0)


@pytest.mark.integration
@pytest.mark.parametrize(
    "src_dataset_fixture",
    [
        {
            "name": "SqliteDataProvider",
        }
    ],
    indirect=True,
    ids=["sqlite"],
)
def test_sqlite_create_table_autodetection_without_file(src_dataset_fixture):
    """Create a table should fail without file or columns"""
    dp, dataset_object = src_dataset_fixture

    statement = f"PRAGMA table_info({dataset_object.name});"
    response = dp.run_sql(statement, handler=lambda x: x.first())
    assert response is None

    with pytest.raises(ValueError) as exc_info:
        dp.create_table(dataset_object)
    assert exc_info.match("File or Dataframe is required for creating table using schema autodetection")
