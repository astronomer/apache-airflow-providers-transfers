from __future__ import annotations

import logging
import os
import random
import string
from dataclasses import dataclass, field
from importlib.metadata import version
from typing import Sequence
from urllib.parse import urlparse

import attr
import pandas as pd
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from packaging import version as packaging_version
from snowflake.connector import pandas_tools
from snowflake.connector.errors import (
    ProgrammingError,
)

from universal_transfer_operator.constants import (
    DEFAULT_CHUNK_SIZE,
    FileLocation,
    FileType,
    LoadExistStrategy,
)
from universal_transfer_operator.data_providers.database.base import DatabaseDataProvider
from universal_transfer_operator.datasets.file.base import File
from universal_transfer_operator.datasets.table import Metadata, Table
from universal_transfer_operator.exceptions import DatabaseCustomError
from universal_transfer_operator.settings import (
    LOAD_TABLE_AUTODETECT_ROWS_COUNT,
    SNOWFLAKE_SCHEMA,
    SNOWFLAKE_STORAGE_INTEGRATION_AMAZON,
    SNOWFLAKE_STORAGE_INTEGRATION_GOOGLE,
)
from universal_transfer_operator.universal_transfer_operator import TransferIntegrationOptions

DEFAULT_STORAGE_INTEGRATION = {
    FileLocation.S3: SNOWFLAKE_STORAGE_INTEGRATION_AMAZON,
    FileLocation.GS: SNOWFLAKE_STORAGE_INTEGRATION_GOOGLE,
}

ASTRO_SDK_TO_SNOWFLAKE_FILE_FORMAT_MAP = {
    FileType.CSV: "CSV",
    FileType.NDJSON: "JSON",
    FileType.PARQUET: "PARQUET",
}

COPY_INTO_COMMAND_FAIL_STATUS = "LOAD_FAILED"

NATIVE_LOAD_SUPPORTED_FILE_TYPES = (FileType.CSV, FileType.NDJSON, FileType.PARQUET)
NATIVE_LOAD_SUPPORTED_FILE_LOCATIONS = (
    FileLocation.GS,
    FileLocation.S3,
)


@dataclass
class SnowflakeStage:
    """
    Dataclass which abstracts properties of a Snowflake Stage.

    Snowflake Stages are used to loading tables and unloading data from tables into files.

    Example:

    .. code-block:: python

        snowflake_stage = SnowflakeStage(
            name="stage_name",
            url="gcs://bucket/prefix",
            metadata=Metadata(database="SNOWFLAKE_DATABASE", schema="SNOWFLAKE_SCHEMA"),
        )

    .. seealso::
            `Snowflake official documentation on stage creation
            <https://docs.snowflake.com/en/sql-reference/sql/create-stage.html>`_
    """

    name: str = ""
    _name: str = field(init=False, repr=False, default="")
    url: str = ""
    metadata: Metadata = field(default_factory=Metadata)

    @staticmethod
    def _create_unique_name() -> str:
        """
        Generate a valid Snowflake stage name.

        :return: unique stage name
        """
        return (
            "stage_"
            + random.choice(string.ascii_lowercase)
            + "".join(random.choice(string.ascii_lowercase + string.digits) for _ in range(7))
        )

    def set_url_from_file(self, file: File) -> None:
        """
        Given a file to be loaded/unloaded to from Snowflake, identifies its folder and
        sets as self.url.

        It is also responsible for adjusting any path specific requirements for Snowflake.

        :param file: File to be loaded/unloaded to from Snowflake
        """
        # the stage URL needs to be the folder where the files are
        # https://docs.snowflake.com/en/sql-reference/sql/create-stage.html#external-stage-parameters-externalstageparams
        url = urlparse(file.location.snowflake_stage_path)
        url = url._replace(path=url.path[: url.path.rfind("/") + 1])
        url = url._replace(scheme="gcs") if url.scheme == "gs" else url
        self.url = url.geturl()

    @property  # type: ignore
    def name(self) -> str:
        """
        Return either the user-defined name or auto-generated one.

        :return: stage name
        :sphinx-autoapi-skip:
        """
        if not self._name:
            self._name = self._create_unique_name()
        return self._name

    @name.setter
    def name(self, value: str) -> None:
        """
        Set the stage name.

        :param value: Stage name.
        """
        if not isinstance(value, property) and value != self._name:
            self._name = value

    @property
    def qualified_name(self) -> str:
        """
        Return stage qualified name. In Snowflake, it is the database, schema and table

        :return: Snowflake stage qualified name (e.g. database.schema.table)
        """
        qualified_name_lists = [
            self.metadata.database,
            self.metadata.schema,
            self.name,
        ]
        qualified_name = ".".join(name for name in qualified_name_lists if name)
        return qualified_name


@attr.define
class SnowflakeOptions(TransferIntegrationOptions):
    """
    Snowflake load option for naive transfer.
    """

    file_options: dict = attr.field(factory=dict)
    copy_options: dict = attr.field(factory=dict)
    storage_integration: str | None = attr.field(default=None)
    validation_mode: str | None = attr.field(default=None)


class SnowflakeDataProvider(DatabaseDataProvider):
    """SnowflakeDataProvider represent all the DataProviders interactions with Snowflake Databases."""

    DEFAULT_SCHEMA = SNOWFLAKE_SCHEMA
    OPTIONS_CLASS = SnowflakeOptions

    def __init__(
        self,
        dataset: Table,
        transfer_mode,
        transfer_params: SnowflakeOptions = SnowflakeOptions(),
    ):
        self.dataset = dataset
        self.transfer_params: SnowflakeOptions = transfer_params
        self.transfer_mode = transfer_mode
        self.transfer_mapping = set()
        self.LOAD_DATA_NATIVELY_FROM_SOURCE: dict = {}
        super().__init__(
            dataset=self.dataset, transfer_mode=self.transfer_mode, transfer_params=self.transfer_params
        )

    def __repr__(self):
        return f'{self.__class__.__name__}(conn_id="{self.dataset.conn_id})'

    @property
    def sql_type(self) -> str:
        return "snowflake"

    @property
    def hook(self) -> SnowflakeHook:
        """Retrieve Airflow hook to interface with the Snowflake database."""
        kwargs = {}
        _hook = SnowflakeHook(snowflake_conn_id=self.dataset.conn_id)
        if self.dataset and self.dataset.metadata:
            if _hook.database is None and self.dataset.metadata.database:
                kwargs.update({"database": self.dataset.metadata.database})
            if _hook.schema is None and self.dataset.metadata.schema:
                kwargs.update({"schema": self.dataset.metadata.schema})
        return SnowflakeHook(snowflake_conn_id=self.dataset.conn_id, **kwargs)

    @property
    def default_metadata(self) -> Metadata:
        """
        Fill in default metadata values for table objects addressing snowflake databases
        """
        connection = self.hook.get_conn()
        return Metadata(  # type: ignore
            schema=connection.schema,
            database=connection.database,
        )

    # ---------------------------------------------------------
    # Table metadata
    # ---------------------------------------------------------
    @staticmethod
    def get_table_qualified_name(table: Table) -> str:  # skipcq: PYL-R0201
        """
        Return table qualified name. In Snowflake, it is the database, schema and table

        :param table: The table we want to retrieve the qualified name for.
        """
        qualified_name_lists = [
            table.metadata.database,
            table.metadata.schema,
            table.name,
        ]
        qualified_name = ".".join(name for name in qualified_name_lists if name)
        return qualified_name

    def schema_exists(self, schema: str) -> bool:
        """
        Checks if a schema exists in the database

        :param schema: DB Schema - a namespace that contains named objects like (tables, functions, etc)
        """

        # Below code is added due to breaking change in apache-airflow-providers-snowflake==3.2.0,
        # we need to pass handler param to get the rows. But in version apache-airflow-providers-snowflake==3.1.0
        # if we pass the handler provider raises an exception AttributeError 'sfid'.
        try:
            schemas = self.hook.run(
                "SELECT SCHEMA_NAME from information_schema.schemata WHERE LOWER(SCHEMA_NAME) = %(schema_name)s;",
                parameters={"schema_name": schema.lower()},
                handler=lambda cur: cur.fetchall(),
            )
        except AttributeError:
            schemas = self.hook.run(
                "SELECT SCHEMA_NAME from information_schema.schemata WHERE LOWER(SCHEMA_NAME) = %(schema_name)s;",
                parameters={"schema_name": schema.lower()},
            )
        if schemas is None:
            raise ValueError("Query didn't returned list of schema")
        try:
            # Handle case for apache-airflow-providers-snowflake<4.0.1
            created_schemas = [x["SCHEMA_NAME"] for x in schemas]
        except TypeError:
            # Handle case for apache-airflow-providers-snowflake>=4.0.1
            created_schemas = [x[0] for x in schemas]
        return len(created_schemas) == 1

    def create_table_using_schema_autodetection(
        self,
        table: Table,
        file: File | None = None,
        dataframe: pd.DataFrame | None = None,
    ) -> None:  # skipcq PYL-W0613
        """
        Create a SQL table, automatically inferring the schema using the given file.
        Overriding default behaviour and not using the `prep_table` since it doesn't allow the adding quotes.

        :param table: The table to be created.
        :param file: File used to infer the new table columns.
        :param dataframe: Dataframe used to infer the new table columns if there is no file
        """
        if file is None:
            if dataframe is None:
                raise ValueError(
                    "File or Dataframe is required for creating table using schema autodetection"
                )
            source_dataframe = dataframe
        else:
            source_dataframe = file.export_to_dataframe(nrows=LOAD_TABLE_AUTODETECT_ROWS_COUNT)

        # We are changing the case of table name to ease out on the requirements to add quotes in raw queries.
        # ToDO - Currently, we cannot to append using load_file to a table name which is having name in lower case.
        pandas_tools.write_pandas(
            conn=self.hook.get_conn(),
            df=source_dataframe,
            table_name=table.name.upper(),
            schema=table.metadata.schema,
            database=table.metadata.database,
            chunk_size=DEFAULT_CHUNK_SIZE,
            quote_identifiers=self.use_quotes(source_dataframe),
            auto_create_table=True,
        )
        # We are truncating since we only expect table to be created with required schema.
        # Since this method is used by both native and pandas path we cannot skip this step.
        self.truncate_table(table)

    def load_pandas_dataframe_to_table(
        self,
        source_dataframe: pd.DataFrame,
        target_table: Table,
        if_exists: LoadExistStrategy = "replace",
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ) -> None:
        """
        Create a table with the dataframe's contents.
        If the table already exists, append or replace the content, depending on the value of `if_exists`.

        :param source_dataframe: Local or remote filepath
        :param target_table: Table in which the file will be loaded
        :param if_exists: Strategy to be used in case the target table already exists.
        :param chunk_size: Specify the number of rows in each batch to be written at a time.
        """
        self._assert_not_empty_df(source_dataframe)

        auto_create_table = False
        if not self.table_exists(target_table):
            auto_create_table = True
        elif if_exists == "replace":
            self.create_table(target_table, dataframe=source_dataframe)

        # We are changing the case of table name to ease out on the requirements to add quotes in raw queries.
        # ToDO - Currently, we cannot to append using load_file to a table name which is having name in lower case.
        pandas_tools.write_pandas(
            conn=self.hook.get_conn(),
            df=source_dataframe,
            table_name=target_table.name.upper(),
            schema=target_table.metadata.schema,
            database=target_table.metadata.database,
            chunk_size=chunk_size,
            quote_identifiers=self.use_quotes(source_dataframe),
            auto_create_table=auto_create_table,
        )

    def truncate_table(self, table):
        """Truncate table"""
        self.run_sql(f"TRUNCATE {self.get_table_qualified_name(table)}")

    @classmethod
    def use_quotes(cls, cols: Sequence[str]) -> bool:
        """
        With snowflake identifier we have two cases,

        1. When Upper/Mixed case col names are used
            We are required to preserver the text casing of the col names. By adding the quotes around identifier.
        2. When lower case col names are used
            We can use them as is

        This is done to be in sync with Snowflake SQLAlchemy dialect.
        https://docs.snowflake.com/en/user-guide/sqlalchemy.html#object-name-case-handling

        Snowflake stores all case-insensitive object names in uppercase text. In contrast, SQLAlchemy considers all
        lowercase object names to be case-insensitive. Snowflake SQLAlchemy converts the object name case during
        schema-level communication (i.e. during table and index reflection). If you use uppercase object names,
        SQLAlchemy assumes they are case-sensitive and encloses the names with quotes. This behavior will cause
        mismatches against data dictionary data received from Snowflake, so unless identifier names have been truly
        created as case sensitive using quotes (e.g. "TestDb"), all lowercase names should be used on the SQLAlchemy
        side.

        :param cols: list of columns
        """
        return any(col for col in cols if not col.islower() and not col.isupper())

    @property
    def openlineage_dataset_name(self) -> str:
        """
        Returns the open lineage dataset name as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        Example: db_name.schema_name.table_name
        """
        conn = self.hook.get_connection(self.dataset.conn_id)
        conn_extra = conn.extra_dejson
        schema = conn_extra.get("schema") or conn.schema
        db = conn_extra.get("database")
        return f"{db}.{schema}.{self.dataset.name}"

    @property
    def openlineage_dataset_namespace(self) -> str:
        """
        Returns the open lineage dataset namespace as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        Example: snowflake://ACCOUNT
        """
        account = self.hook.get_connection(self.dataset.conn_id).extra_dejson.get("account")
        return f"{self.sql_type}://{account}"

    @property
    def openlineage_dataset_uri(self) -> str:
        """
        Returns the open lineage dataset uri as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        """
        return f"{self.openlineage_dataset_namespace}{self.openlineage_dataset_name}"

    def is_native_path_available(
        self,
        source_dataset: File | Table,  # skipcq PYL-W0613, PYL-R0201
    ) -> bool:
        """
        Check if there is an optimised path for source to destination.

        :param source_dataset: File | Table from which we need to transfer data
        """
        if isinstance(source_dataset, Table):
            return False
        is_file_type_supported = source_dataset.type.name in NATIVE_LOAD_SUPPORTED_FILE_TYPES
        is_file_location_supported = (
            source_dataset.location.location_type in NATIVE_LOAD_SUPPORTED_FILE_LOCATIONS
        )
        return is_file_type_supported and is_file_location_supported

    def load_file_to_table_natively(
        self,
        source_file: File,
        target_table: Table,
        if_exists: LoadExistStrategy = "replace",
        native_support_kwargs: dict | None = None,
        **kwargs,
    ):  # skipcq PYL-W0613
        """
        Load the content of a file to an existing Snowflake table natively by:
        - Creating a Snowflake external stage
        - Using Snowflake COPY INTO statement

        Requirements:
        - The user must have permissions to create a STAGE in Snowflake.
        - If loading from GCP Cloud Storage, `native_support_kwargs` must define `storage_integration`
        - If loading from AWS S3, the credentials for creating the stage may be
        retrieved from the Airflow connection or from the `storage_integration`
        attribute within `native_support_kwargs`.

        :param source_file: File from which we need to transfer data
        :param target_table: Table to which the content of the file will be loaded to
        :param if_exists: Strategy used to load (currently supported: "append" or "replace")
        :param native_support_kwargs: may be used for the stage creation, as described above.

        .. seealso::
            `Snowflake official documentation on COPY INTO
            <https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html>`_
            `Snowflake official documentation on CREATE STAGE
            <https://docs.snowflake.com/en/sql-reference/sql/create-stage.html>`_

        """
        native_support_kwargs = native_support_kwargs or {}
        # if not self.load_options:
        #     self.load_options = SnowflakeLoadOptions()

        storage_integration = native_support_kwargs.get("storage_integration", None)
        if storage_integration is None:
            storage_integration = self.transfer_params.storage_integration

        stage = self.create_stage(file=source_file, storage_integration=storage_integration)

        rows = self._copy_into_table_from_stage(
            source_file=source_file, target_table=target_table, stage=stage
        )
        self.evaluate_results(rows)

    @staticmethod
    def _create_stage_auth_sub_statement(file: File, storage_integration: str | None = None) -> str:
        """
        Create authentication-related line for the Snowflake CREATE STAGE.
        Raise an exception if it is not defined.

        :param file: File to be copied from/to using stage
        :param storage_integration: Previously created Snowflake storage integration
        :return: String containing line to be used for authentication on the remote storage
        """
        storage_integration = storage_integration or DEFAULT_STORAGE_INTEGRATION.get(
            file.location.location_type
        )
        if storage_integration is not None:
            return f"storage_integration = {storage_integration};"
        return file.location.get_snowflake_stage_auth_sub_statement()  # type: ignore

    def create_stage(
        self,
        file: File,
        storage_integration: str | None = None,
        metadata: Metadata | None = None,
    ) -> SnowflakeStage:
        """
        Creates a new named external stage to use for loading data from files into Snowflake
        tables and unloading data from tables into files.

        At the moment, the following ways of authenticating to the backend are supported:
        * Google Cloud Storage (GCS): using storage_integration, previously created
        * Amazon (S3): one of the following:
        (i) using storage_integration or
        (ii) retrieving the AWS_KEY_ID and AWS_SECRET_KEY from the Airflow file connection

        :param file: File to be copied from/to using stage
        :param storage_integration: Previously created Snowflake storage integration
        :param metadata: Contains Snowflake database and schema information
        :return: Stage created

        .. seealso::
            `Snowflake official documentation on stage creation
            <https://docs.snowflake.com/en/sql-reference/sql/create-stage.html>`_
        """

        auth = self._create_stage_auth_sub_statement(file=file, storage_integration=storage_integration)

        # if not self.load_options:
        #     self.load_options = SnowflakeLoadOptions()
        metadata = metadata or self.default_metadata
        stage = SnowflakeStage(metadata=metadata)
        stage.set_url_from_file(file)

        file_format = ASTRO_SDK_TO_SNOWFLAKE_FILE_FORMAT_MAP[file.type.name]
        copy_options = []
        copy_options.extend([f"{k}={v}" for k, v in self.transfer_params.copy_options.items()])
        file_options = [f"{k}={v}" for k, v in self.transfer_params.file_options.items()]
        file_options.extend([f"TYPE={file_format}", "TRIM_SPACE=TRUE"])
        file_options_str = ", ".join(file_options)
        copy_options_str = ", ".join(copy_options)
        sql_statement = "".join(
            [
                f"CREATE OR REPLACE STAGE {stage.qualified_name} URL='{stage.url}' ",
                f"FILE_FORMAT=({file_options_str}) ",
                f"COPY_OPTIONS=({copy_options_str}) ",
                auth,
            ]
        )
        logging.debug("SQL statement executed: %s ", sql_statement)
        self.run_sql(sql_statement)

        return stage

    def _copy_into_table_from_stage(self, source_file, target_table, stage):
        table_name = self.get_table_qualified_name(target_table)
        file_path = os.path.basename(source_file.path) or ""
        sql_statement = f"COPY INTO {table_name} FROM @{stage.qualified_name}/{file_path}"

        self._validate_before_copy_into(source_file, target_table, stage)

        # Below code is added due to breaking change in apache-airflow-providers-snowflake==3.2.0,
        # we need to pass handler param to get the rows. But in version apache-airflow-providers-snowflake<=3.1.0
        # if we pass the handler provider raises an exception AttributeError
        try:
            if packaging_version.parse(
                version("apache-airflow-providers-snowflake")
            ) <= packaging_version.parse("3.1.0"):
                rows = self.hook.run(sql_statement)
            else:
                rows = self.hook.run(sql_statement, handler=lambda cur: cur.fetchall())
        except ValueError as exe:
            raise DatabaseCustomError from exe
        finally:
            self.drop_stage(stage)
        return rows

    def _validate_before_copy_into(
        self, source_file: File, target_table: Table, stage: SnowflakeStage
    ) -> None:
        """Validate COPY INTO command to tests the files for errors but does not load them."""
        if self.transfer_params:
            if self.transfer_params.validation_mode is None:
                return
            table_name = self.get_table_qualified_name(target_table)
            file_path = os.path.basename(source_file.path) or ""
            sql_statement = (
                f"COPY INTO {table_name} FROM "
                f"@{stage.qualified_name}/{file_path} VALIDATION_MODE='{self.transfer_params.validation_mode}'"
            )
            try:
                self.hook.run(sql_statement, handler=lambda cur: cur.fetchall())
            except ProgrammingError as load_exception:
                self.drop_stage(stage)
                raise load_exception

    def drop_stage(self, stage: SnowflakeStage) -> None:
        """
        Runs the snowflake query to drop stage if it exists.

        :param stage: Stage to be dropped
        """
        sql_statement = f"DROP STAGE IF EXISTS {stage.qualified_name};"
        self.hook.run(sql_statement, autocommit=True)

    @staticmethod
    def evaluate_results(rows):
        """check the error state returned by snowflake when running `copy into` query."""
        try:
            # Handle case for apache-airflow-providers-snowflake<4.0.1
            if any(row["status"] == COPY_INTO_COMMAND_FAIL_STATUS for row in rows):
                raise DatabaseCustomError(rows)
        except TypeError:
            # Handle case for apache-airflow-providers-snowflake>=4.0.1
            if any(row[0] == COPY_INTO_COMMAND_FAIL_STATUS for row in rows):
                raise DatabaseCustomError(rows)
