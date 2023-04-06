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
