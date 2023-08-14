from __future__ import annotations

import io

import pandas as pd

from universal_transfer_operator.constants import FileType as FileTypeConstants
from universal_transfer_operator.datasets.file.types.base import FileTypes


class JSONFileTypes(FileTypes):
    """Concrete implementation to handle JSON file type"""

    # We need skipcq because it's a method overloading so we don't want to make it a static method
    def export_to_dataframe(
        self,
        stream: io.TextIOWrapper,
        columns_names_capitalization="original",
        **kwargs,
    ) -> pd.DataFrame:  # skipcq PYL-R0201
        """
        Read json file from one of the supported locations and return dataframe

        :param stream: file stream object
        :param columns_names_capitalization: determines whether to convert all columns to lowercase/uppercase
            in the resulting dataframe
        """
        kwargs_copy = dict(kwargs)
        # Pandas `read_json` does not support the `nrows` parameter unless we're using NDJSON
        kwargs_copy.pop("nrows", None)
        return pd.read_json(stream, **kwargs_copy)

    # We need skipcq because it's a method overloading so we don't want to make it a static method
    def create_from_dataframe(self, df: pd.DataFrame, stream: io.TextIOWrapper) -> None:  # skipcq PYL-R0201
        """
        Write json file to one of the supported locations

        :param df: pandas dataframe
        :param stream: file stream object
        """
        df.to_json(stream, orient="records")

    @property
    def name(self):
        return FileTypeConstants.JSON
