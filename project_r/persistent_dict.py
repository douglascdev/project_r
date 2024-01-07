import json
import logging
from io import StringIO, TextIOBase
from pathlib import Path
from typing import Any

from project_r.decorators import ensure_file_is_open
from project_r.metadata import MetadataController
from project_r.rwlock import RWLock

__all__ = ["PersistentDict"]


class PersistentDict:
    """
    Persistent async key-value storage.

    :param values_file: file where values are stored. This can be a Path
    to a database file or a TextIOBase if you don't want to use a file,
    such as using a StringIO object for a memory database.

    :param metadata_file: file where metadata is stored. Same type as values_file,
    but defaults to copying the same path as values_file with the same
    name, but removing the extension and appending "_metadata.json", or to an
    empty StringIO if values_file is a TextIOBase instead of a Path.
    """

    def __init__(
        self,
        values_file: TextIOBase | Path,
        metadata_file: TextIOBase | Path | None = None,
    ) -> None:
        if metadata_file is None:
            if isinstance(values_file, Path):
                metadata_file = values_file.with_name(
                    values_file.name + "_metadata.json"
                )
            else:
                metadata_file = StringIO("")

        self._metadata = MetadataController(self._get_file_object(metadata_file))
        self._values_file: TextIOBase = self._get_file_object(values_file)
        self._rwlock = RWLock()

    def _get_file_object(self, file) -> TextIOBase:
        if type(file) is Path:
            return open(file, "r+" if file.is_file() else "w+")
        elif isinstance(file, TextIOBase):
            return file
        else:
            raise Exception("Invalid file type")

    @ensure_file_is_open
    async def get(self, key) -> Any:
        async with self._rwlock.reader_locked():
            if value_location := self._metadata.get(key):
                start_i, end_i = value_location
                self._values_file.seek(start_i)
                result_json = self._values_file.read(end_i - start_i)
                return json.loads(result_json)

            return None

    @ensure_file_is_open
    async def set(self, key, value):
        async with self._rwlock.writer_locked():
            logging.debug(f"Persistent storage set {key} to {value}")

            value_json = json.dumps(value, sort_keys=True)
            self._metadata.set(key, len(value_json))
            pos = self._metadata.get(key)
            assert (
                pos is not None
            ), f"Failed to get position for key {key} and value {value}"

            start_i, _ = pos

            self._values_file.seek(start_i)
            chars_written = self._values_file.write(value_json)
            assert chars_written == len(
                value_json
            ), f"Wrote {chars_written} characters instead of {len(value_json)}"

            self._values_file.flush()

    @ensure_file_is_open
    async def remove(self, key):
        async with self._rwlock.writer_locked():
            logging.debug(f"Persistent storage removed {key}")
            self._metadata.remove(key)

    def close(self):
        if not self._values_file.closed:
            self._values_file.close()
            self._data = {}

    def __del__(self):
        """
        Ensure the file is closed when the object is destroyed.

        May have unexpected behaviour
        """
        self.close()
