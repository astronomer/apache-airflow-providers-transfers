import random
import string

import pandas as pd
from pandas.testing import assert_frame_equal

import smart_open
from pyarrow.lib import ArrowInvalid
from urllib.parse import urlparse, urlunparse
from universal_transfer_operator.datasets.file.base import File
from universal_transfer_operator.datasets.table import Table


def create_unique_str(length: int = 50) -> str:
    """
    Create a unique table name of the requested size, which is compatible with all supported databases.
    :return: Unique table name
    :rtype: str
    """
    unique_id = random.choice(string.ascii_lowercase) + "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in range(length - 1)
    )
    return unique_id


def assert_dataframes_are_equal(df: pd.DataFrame, expected: pd.DataFrame) -> None:
    """
    Auxiliary function to compare similarity of dataframes to avoid repeating this logic in many tests.
    """
    df = df.rename(columns=str.lower)
    df = df.astype({"id": "int64"})
    expected = expected.astype({"id": "int64"})
    assert_frame_equal(df, expected)




def export_to_dataframe(data_provider) -> pd.DataFrame:
    """Read file from all supported location and convert them into dataframes."""
    if isinstance(data_provider.dataset, File):
        path = data_provider.dataset.path
        # Currently, we are passing the credentials to sftp server via URL - sftp://username:password@localhost, we are
        # populating the credentials in the URL if the server destination is SFTP.
        if data_provider.dataset.path.startswith("sftp://"):
            path = get_complete_url(data_provider)
        try:
            # Currently, there is a limitation, when we are saving data of a table in a file, we choose rhe parquet
            # format, when moving this saved file to another filetype location(like s3/gcs/local) we are not able to
            # change the data format, because of this case when validating if the source is a database and
            # destination is a filetype, we need to check for parquet format, for other cases like -
            # database -> database, filesystem -> database and filesystem -> filesystem it works as expected.
            with smart_open.open(path, mode="rb", transport_params=data_provider.transport_params) as stream:
                return pd.read_parquet(stream)
        except ArrowInvalid:
            with smart_open.open(path, mode="r", transport_params=data_provider.transport_params) as stream:
                return pd.read_csv(stream)
    elif isinstance(data_provider.dataset, Table):
        return data_provider.export_table_to_pandas_dataframe()


def get_complete_url(dataset_provider):
    """
    Add sftp credential to url
    """
    path = dataset_provider.dataset.path
    original_url = urlparse(path)
    cred_url = urlparse(dataset_provider.get_uri())
    url_netloc = f"{cred_url.netloc}/{original_url.netloc}"
    url_path = original_url.path
    cred_url = cred_url._replace(netloc=url_netloc, path=url_path)
    path = urlunparse(cred_url)
    return path