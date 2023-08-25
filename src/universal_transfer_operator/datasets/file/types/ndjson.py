from __future__ import annotations

import io
import json

import pandas as pd

from universal_transfer_operator.constants import DEFAULT_CHUNK_SIZE
from universal_transfer_operator.constants import FileType as FileTypeConstants
from universal_transfer_operator.datasets.file.types.base import FileTypes


class NDJsonFileTypes(FileTypes):
    """Concrete implementation to handle ndjson file type"""

    def export_to_dataframe(self, stream, columns_names_capitalization="original", **kwargs):
        """
        Read ndjson file from one of the supported locations and return dataframe

        :param stream: file stream object
        :param columns_names_capitalization: determines whether to convert all columns to lowercase/uppercase
            in the resulting dataframe
        """
        return NDJsonFileTypes.flatten(self.normalize_config, stream, **kwargs)

    # We need skipcq because it's a method overloading so we don't want to make it a static method
    def create_from_dataframe(self, df: pd.DataFrame, stream: io.TextIOWrapper) -> None:  # skipcq PYL-R0201
        """
        Write ndjson file to one of the supported locations

        :param df: pandas dataframe
        :param stream: file stream object
        """
        df.to_json(stream, orient="records", lines=True)

    @property
    def name(self):
        return FileTypeConstants.NDJSON

    @staticmethod
    def flatten(normalize_config: dict | None, stream: io.TextIOWrapper, **kwargs) -> pd.DataFrame:
        """
        Flatten the nested ndjson/json.

        :param normalize_config: parameters in dict format of pandas json_normalize() function.
            https://pandas.pydata.org/docs/reference/api/pandas.json_normalize.html
        :param stream: io.TextIOWrapper object for the file
        """
        normalize_config = normalize_config or {}
        nrows = kwargs.get("nrows", float("inf"))
        chunksize = kwargs.get("chunksize", DEFAULT_CHUNK_SIZE)

        result_df = []
        row_count = 0
        extra_rows = []

        while nrows and row_count < nrows:
            extra_rows.extend(stream.readlines(chunksize))
            rows = extra_rows
            if len(rows) == 0:
                break

            # Remove extra rows
            if nrows and nrows < row_count + len(rows):
                extra_rows = rows[nrows:]
                rows = rows[:nrows]
            else:
                extra_rows = []

            df = pd.json_normalize([json.loads(row) for row in rows], **normalize_config)
            result_df.append(df)

            row_count = row_count + df.shape[0]

        # Running concat multiple times is much slower instead we are appending all the generated dataframes
        # in a list and then concatenating in single call. This brought down the cost from 351.79550790786743
        # to 2.3778765201568604.
        return pd.concat(result_df)
