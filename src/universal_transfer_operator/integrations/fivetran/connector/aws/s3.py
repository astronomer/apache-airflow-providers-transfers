from __future__ import annotations

import json
import logging
import uuid
from typing import Any

import boto3
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from universal_transfer_operator.integrations.fivetran.connector.base import Dataset, FivetranConnector
from universal_transfer_operator.settings import FIVETRAN_AWS_VPC_ACCOUNT_ID


class S3Connector(FivetranConnector):
    """
    Fivetran connector details. More details at: https://fivetran.com/docs/rest-api/connectors/config#amazons3

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
        connector_id: str | None,
        service: str,
        config: dict,
        paused: bool = False,
        pause_after_trial: bool = False,
        sync_frequency: str | None = None,
        daily_sync_time: str | None = None,
        schedule_type: str | None = None,
        trust_certificates: bool = False,
        trust_fingerprints: bool = False,
        run_setup_tests: bool = True,
        connect_card_config: dict | None = None,
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
        super().__init__(
            connector_id=self.connector_id,
            service=self.service,
            config=self.config,
            paused=self.paused,
            pause_after_trial=self.pause_after_trial,
            sync_frequency=self.sync_frequency,
            daily_sync_time=self.daily_sync_time,
            schedule_type=self.schedule_type,
            trust_certificates=self.trust_certificates,
            trust_fingerprints=self.trust_fingerprints,
            run_setup_tests=self.run_setup_tests,
            connect_card_config=self.connect_card_config,
        )

    def to_dict(self) -> dict:
        """
        Convert options class to dict
        """
        return vars(self)

    def _hook(self, conn_id: str, verify: Any, transfer_config_args: Any, s3_extra_args: Any) -> S3Hook:
        """Return an instance of the database-specific Airflow hook."""
        return S3Hook(
            aws_conn_id=conn_id,
            verify=verify,
            transfer_config_args=transfer_config_args,
            extra_args=s3_extra_args,
        )

    def map_airflow_connection_to_fivetran(
        self,
        source_dataset: Dataset,
        destination_dataset: Dataset,
        group_id: str,
    ) -> dict[str, str]:
        """
        Maps the airflow connection details to fivetran config for destination

        :param source_dataset: Source dataset
        :param destination_dataset: Destination dataset
        :param group_id: Group id in fivetran system
        """
        conn = BaseHook().get_connection(source_dataset.conn_id)  # type: ignore

        access_key = conn.login
        secret_key = conn.password

        if not access_key:
            raise ConnectionError(f"AWS_ACCESS_KEY_ID is not configured for {source_dataset.conn_id}")  # type: ignore

        if not secret_key:
            raise ConnectionError(f"AWS_SECRET_ACCESS_KEY is not configured {source_dataset.conn_id}")  # type: ignore

        s3_hook = self._hook(
            conn_id=source_dataset.conn_id,  # type: ignore
            verify=self._verify(source_dataset),
            transfer_config_args=self._transfer_config_args(source_dataset),
            s3_extra_args=self._s3_extra_args(source_dataset),
        )

        bucket_name = self._bucket_name(s3_hook=s3_hook, dataset=source_dataset)
        # Create IAM policy. Read more at: Read more at: https://fivetran.com/docs/files/amazon-s3/setup-guide
        iam_policy = self._create_aws_policy(
            bucket_name=bucket_name, access_key=access_key, secret_key=secret_key
        )

        # Create IAM Role. Read more at: Read more at: https://fivetran.com/docs/files/amazon-s3/setup-guide
        iam_role = self._create_role(
            role_name=self._generate_unique_fivetran_name(),
            fivetran_aws_vpc_account_id=str(FIVETRAN_AWS_VPC_ACCOUNT_ID),
            group_id=group_id,
            access_key=access_key,
            secret_key=secret_key,
        )

        # Attach IAM policy to role. Read more at: Read more at: https://fivetran.com/docs/files/amazon-s3/setup-guide
        self._attach_policy_to_role(
            role_name=iam_role["Role"]["RoleName"],
            policy_arn=iam_policy["Policy"]["Arn"],
            access_key=access_key,
            secret_key=secret_key,
        )

        connection_details = {
            "schema": destination_dataset.metadata.schema,  # type: ignore
            "table": destination_dataset.name,  # type: ignore
            "external_id": group_id,
            "is_public": self.config.get("is_public"),
            "role_arn": iam_role["Role"]["Arn"],
            "bucket": bucket_name,
            "prefix": self._prefix(dataset=source_dataset),
            "file_type": self.config.get("file_type"),
            "compression": self.config.get("compression"),
            "on_error": self.config.get("on_error"),
        }

        return connection_details

    @staticmethod
    def _verify(dataset: Dataset) -> Any:
        return dataset.extra.get("verify")  # type: ignore

    @staticmethod
    def _transfer_config_args(dataset: Dataset) -> Any:
        return dataset.extra.get("transfer_config_args", {})  # type: ignore

    @staticmethod
    def _s3_extra_args(dataset: Dataset) -> Any:
        return dataset.extra.get("s3_extra_args", {})  # type: ignore

    @staticmethod
    def _bucket_name(s3_hook: S3Hook, dataset: Dataset) -> Any:
        bucket_name, _ = s3_hook.parse_s3_url(dataset.path)  # type: ignore
        return bucket_name

    @staticmethod
    def _s3_key(s3_hook: S3Hook, dataset: Dataset) -> Any:
        _, key = s3_hook.parse_s3_url(dataset.path)  # type: ignore
        return key

    @staticmethod
    def _prefix(dataset: Dataset) -> Any:
        return dataset.extra.get("prefix", None)  # type: ignore

    @staticmethod
    def _delimiter(dataset: Dataset) -> Any:
        return dataset.extra.get("delimiter", None)  # type: ignore

    @staticmethod
    def _generate_unique_fivetran_name() -> str:
        """Generates the unique fivetran name based on uuid."""
        uuid_value = str(uuid.uuid4())[:8]
        unique_policy_name = f"fivetran-{uuid_value}"
        return unique_policy_name

    def _create_aws_policy(self, bucket_name: str, access_key: str, secret_key: str):
        """
        Creates the AWS IAM policy to read and list the S3 Bucket.
        Read more at: https://fivetran.com/docs/files/amazon-s3/setup-guide

        :param bucket_name: Name of bucket
        :param access_key: AWS access key
        :param secret_key: AWS secret key
        """
        # Create IAM client
        iam = boto3.client("iam", aws_access_key_id=access_key, aws_secret_access_key=secret_key)

        # Create a policy
        my_managed_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": ["s3:GetObject", "s3:ListBucket"],
                    "Resource": [f"arn:aws:s3:::{bucket_name}/*", f"arn:aws:s3:::{bucket_name}"],
                }
            ],
        }
        from botocore.exceptions import ClientError

        try:
            response = iam.create_policy(
                PolicyName=self._generate_unique_fivetran_name(), PolicyDocument=json.dumps(my_managed_policy)
            )
        except ClientError:
            logging.error(f"Couldn't create policy with config {my_managed_policy}")
            raise
        return response

    @staticmethod
    def _create_role(
        role_name: str, fivetran_aws_vpc_account_id: str, group_id: str, access_key: str, secret_key: str
    ):
        """
        Creates a role that lets a list of specified services assume the role.
        Read more at: https://fivetran.com/docs/files/amazon-s3/setup-guide

        :param role_name: The name of the role.
        :param fivetran_aws_vpc_account_id: Fivetran’s AWS VPC Account ID
        :param group_id: Group id in fivetran system
        :param access_key: AWS access key
        :param secret_key: AWS secret key
        """
        # Create IAM client
        iam = boto3.client("iam", aws_access_key_id=access_key, aws_secret_access_key=secret_key)

        trust_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": f"arn:aws:iam::{fivetran_aws_vpc_account_id}:root"},
                    "Action": "sts:AssumeRole",
                    "Condition": {"StringEquals": {"sts:ExternalId": f"{group_id}"}},
                }
            ],
        }
        from botocore.exceptions import ClientError

        try:
            role = iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=json.dumps(trust_policy))
            logging.info(f"role created with name {role_name}")
        except ClientError:
            logging.error(f"Couldn't create role {role_name}")
            raise
        else:
            return role

    @staticmethod
    def _attach_policy_to_role(role_name: str, policy_arn: str, access_key: str, secret_key: str):
        """
        Attaches a policy to a role.

        :param role_name: The name of the role. **Note** this is the name, not the ARN.
        :param policy_arn: The ARN of the policy.
        :param access_key: AWS access key
        :param secret_key: AWS secret key
        """
        iam = boto3.client("iam", aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        from botocore.exceptions import ClientError

        try:
            # Attach a role policy
            iam.attach_role_policy(PolicyArn=policy_arn, RoleName=role_name)
        except ClientError:
            logging.error(f"Couldn't attach policy {policy_arn} to role {role_name}")
            raise
