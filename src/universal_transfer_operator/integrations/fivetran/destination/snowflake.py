from __future__ import annotations

from airflow.hooks.base import BaseHook

from universal_transfer_operator.integrations.fivetran.destination.base import FivetranDestination


class SnowflakeDestination(FivetranDestination):
    """Snowflake destination class to transfer datasets using Fivetran APIs."""

    def __init__(
        self,
        service: str,
        config: dict,
        destination_id: str | None = None,
        time_zone_offset: str | None = "-5",
        region: str | None = "US",
        run_setup_tests: bool | None = True,
    ):
        self.service = service
        self.config = config
        self.destination_id = destination_id
        self.time_zone_offset = time_zone_offset
        self.region = region
        self.run_setup_tests = run_setup_tests
        super().__init__(
            service=self.service,
            config=self.config,
            destination_id=self.destination_id,
            time_zone_offset=self.time_zone_offset,
            region=self.region,
            run_setup_tests=self.run_setup_tests,
        )

    def map_airflow_connection_to_fivetran(
        self,
        conn_id: str = "snowflake_conn_id",
        database_overridden: str | None = None,
        schema_overridden: str | None = None,
    ) -> dict[str, str]:
        """
        Maps the airflow snowflake connection details to fivetran config for destination. Read more:
        https://fivetran.com/docs/rest-api/destinations/config#snowflake
        https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/connections/snowflake.html

        :param conn_id: Airflow connection ID
        :param database_overridden: Database name to be overridden
        :param schema_overridden: Schema name to be overridden
        """
        conn = BaseHook().get_connection(conn_id)
        extras = {
            "account": "account",
            "database": "database",
            "region": "region",
            "role": "role",
            "warehouse": "warehouse",
        }
        snowflake_dejson = conn.extra_dejson
        # At some point, the prefix extra__snowflake__ for the keys in extras was removed in a provider release.
        # We handle the backward compatibility here by prepending the required prefix to the extras keys.
        if snowflake_dejson.get("account") is None:
            for key, value in extras.items():
                extras[key] = f"extra__snowflake__{value}"
        account = self._get_snowflake_account(
            snowflake_dejson.get(extras["account"]), snowflake_dejson.get(extras["region"])  # type: ignore
        )

        connection_details = {
            "host": f"{account}.snowflakecomputing.com",
            "port": conn.port if conn.port else 443,
            "database": database_overridden
            if database_overridden
            else conn.extra_dejson.get(extras["database"]),
            "auth": "PASSWORD",
            "user": conn.login,
            "password": conn.password,
            "role": snowflake_dejson.get(extras["role"]),
            "warehouse": snowflake_dejson.get(extras["warehouse"]),
            "schema": schema_overridden if schema_overridden else conn.schema,
            "account": account,
        }

        return connection_details

    @staticmethod
    def _get_snowflake_account(account: str, region: str | None = None) -> str:
        """
        Create the snowflake account from account and region.

        :param account: Snowflake account name
        :param region: Snowflake region name
        """
        # Region is optional
        if region and region not in account:
            account = f"{account}.{region}"
        return account
