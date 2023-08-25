from __future__ import annotations

import io
import os
from abc import abstractmethod
from pathlib import Path
from typing import Iterator

import attr
import pandas as pd
import smart_open
from airflow.hooks.base import BaseHook

from universal_transfer_operator.constants import FileType, Location, TransferMode
from universal_transfer_operator.data_providers.base import DataProviders, DataStream
from universal_transfer_operator.datasets.file.base import File
from universal_transfer_operator.datasets.file.types import create_file_type
from universal_transfer_operator.universal_transfer_operator import TransferIntegrationOptions
from universal_transfer_operator.utils import get_dataset_connection_type


@attr.define
class TempFile:
    tmp_file: Path | None
    actual_filename: Path


class BaseFilesystemProviders(DataProviders[File]):
    """BaseFilesystemProviders represent all the DataProviders interactions with File system."""

    def __init__(
        self,
        dataset: File,
        transfer_mode,
        transfer_params: TransferIntegrationOptions = TransferIntegrationOptions(),
    ):
        self.dataset = dataset
        self.transfer_params = transfer_params
        self.transfer_mode = transfer_mode
        self.transfer_mapping: set[Location] = set()
        self.LOAD_DATA_NATIVELY_FROM_SOURCE: dict = {}
        super().__init__(dataset=self.dataset)

    def __repr__(self):
        return f'{self.__class__.__name__}(conn_id="{self.dataset.conn_id})'

    @property
    def hook(self) -> BaseHook:
        """Return an instance of the database-specific Airflow hook."""
        raise NotImplementedError

    @property
    @abstractmethod
    def paths(self) -> list[str]:
        """Resolve patterns in path"""
        raise NotImplementedError

    def delete(self, path: str):  # type: ignore
        """
        Delete a file/object if they exists
        """
        raise NotImplementedError

    @property
    def transport_params(self) -> dict | None:  # skipcq: PYL-R0201
        """Get credentials required by smart open to access files"""
        return None

    def check_if_exists(self) -> bool:
        """Return true if the dataset exists"""
        raise NotImplementedError

    def exists(self) -> bool:
        return self.check_if_exists()

    def check_if_transfer_supported(self, source_dataset: File) -> bool:
        """
        Checks if the transfer is supported from source to destination based on source_dataset.
        """
        source_connection_type = get_dataset_connection_type(source_dataset)
        return Location(source_connection_type) in self.transfer_mapping

    def read(self) -> Iterator[DataStream]:
        """Read the remote or local file dataset and returns i/o buffers"""
        if self.transfer_mode == TransferMode.NATIVE:
            return iter(
                [
                    DataStream(
                        actual_file=self.dataset, remote_obj_buffer=io.BytesIO(), actual_filename=Path("")
                    )
                ]
            )
        else:
            return self.read_using_smart_open()

    def read_using_smart_open(self) -> Iterator[DataStream]:
        """Read the file dataset using smart open returns i/o buffer"""
        files = self.paths
        for file in files:
            yield DataStream(
                remote_obj_buffer=self._convert_remote_file_to_byte_stream(file),
                actual_filename=Path(file),
                actual_file=self.dataset,
            )

    def _convert_remote_file_to_byte_stream(self, file: str) -> io.IOBase:
        """
        Read file from all supported location and convert them into a buffer that can be streamed into other data
        structures.

        :returns: an io object that can be streamed into a dataframe (or other object)
        """
        mode = "rb" if self.read_as_binary(file) else "r"
        remote_obj_buffer = io.BytesIO() if self.read_as_binary(file) else io.StringIO()
        with smart_open.open(file, mode=mode, transport_params=self.transport_params) as stream:
            remote_obj_buffer.write(stream.read())
            remote_obj_buffer.seek(0)
            return remote_obj_buffer

    def write(self, source_ref: DataStream | pd.DataFrame) -> str:
        """
        Write the data from local reference location or a dataframe to the filesystem dataset or database dataset

        :param source_ref: Source DataStream object which will be used to read data
        """
        return self.write_using_smart_open(source_ref=source_ref)

    def write_using_smart_open(self, source_ref: DataStream | pd.DataFrame) -> str:
        """Write the source data from remote object i/o buffer to the dataset using smart open"""
        if isinstance(source_ref, DataStream):
            # `source_ref` can be a dataframe for all the filetypes we can create a dataframe for like -
            # CSV, JSON, NDJSON, and Parquet or SQL Tables. This gives us the option to perform various
            # functions on the data on the fly, like filtering or changing the file format altogether. For other
            # files whose content cannot be converted to dataframe like - zip or image, we get a DataStream object.
            return self.write_from_file(source_ref)
        else:
            return self.write_from_dataframe(source_ref)

    def write_from_file(self, source_ref: DataStream) -> str:
        """Write the remote object i/o buffer to the dataset using smart open
        :param source_ref: DataStream object of source dataset
        :return: File path that is the used for write pattern
        """
        mode = "wb" if self.read_as_binary(source_ref.actual_file.path) else "w"

        destination_file = self.dataset.path
        # check if destination dataset is folder or file pattern
        if self.dataset.is_pattern():
            destination_file = os.path.join(self.dataset.path, os.path.basename(source_ref.actual_filename))

        with smart_open.open(destination_file, mode=mode, transport_params=self.transport_params) as stream:
            stream.write(source_ref.remote_obj_buffer.read())
        return destination_file

    def write_from_dataframe(self, source_ref: pd.DataFrame) -> str:
        """Write the dataframe to the SFTP dataset using smart open
        :param source_ref: DataStream object of source dataset
        :return: File path that is the used for write pattern
        """
        mode = "wb" if self.read_as_binary(self.dataset.path) else "w"

        destination_file = self.dataset.path
        with smart_open.open(destination_file, mode=mode, transport_params=self.transport_params) as stream:
            self.dataset.type.create_from_dataframe(stream=stream, df=source_ref)
        return destination_file

    def read_as_binary(self, file: str) -> bool:
        """
        Checks if file has to be read as binary or as string i/o.

        :return: True or False
        """
        try:
            filetype = create_file_type(
                path=file,
                filetype=self.dataset.filetype,
                normalize_config=self.dataset.normalize_config,
            )
        except ValueError:
            # return True when the extension of file is not supported
            # Such file can be read as binary and transferred.
            return True

        read_as_non_binary = {FileType.CSV, FileType.JSON, FileType.NDJSON}
        if filetype in read_as_non_binary:
            return False
        return True

    @staticmethod
    def cleanup(file_list: list[TempFile]) -> None:
        """Cleans up the temporary files created"""
        for file in file_list:
            if os.path.exists(file.actual_filename):
                os.remove(file.actual_filename)

    @property
    def openlineage_dataset_namespace(self) -> str:
        """
        Returns the open lineage dataset namespace as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        """
        raise NotImplementedError

    @property
    def openlineage_dataset_name(self) -> str:
        """
        Returns the open lineage dataset name as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        """
        raise NotImplementedError

    @property
    def openlineage_dataset_uri(self) -> str:
        """
        Returns the open lineage dataset uri as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def size(self) -> int:
        """Return the size in bytes of the given file"""
        raise NotImplementedError

    @property
    def snowflake_stage_path(self) -> str:
        """
        Get the altered path if needed for stage creation in snowflake stage creation
        """
        return self.dataset.path

    def is_native_path_available(
        self,
        source_dataset: File,  # skipcq PYL-W0613, PYL-R0201
    ) -> bool:
        """
        Check if there is an optimised path for source to destination.

        :param source_dataset: File from which we need to transfer data
        """
        return False
