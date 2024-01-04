import json
import logging
from io import TextIOBase
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generic, Type, TypedDict, TypeVar

from project_r.decorators import ensure_file_is_open
from project_r.rwlock import RWLock

__all__ = ["PersistentDict"]

T = TypeVar("T")
# T = TypeVar("T", bound=TypedDict)


class PersistentDict(Generic[T]):
    """
    Persistent async key-value storage.

    Generic type T is an optional TypedDict you can write that enables type hints.

    Filecan be a Path to a database file or a TextIOBase if you don't want to use a file,
    such as using a StringIO object for a memory database.
    A metadata json file will be created automatically in the same directory with the same
    name, but removing the extension and appending "_metadata.json".

    Example:

    ```python
    class MyData(TypedDict):
        name: str

    storage = PersistentDict[MyData](file=Path("/path/to/file.aks"))
    # Since we are using a TypedDict, your IDE will complete name as a string
    name = await storage.get("name")

    # This would work, but without providing type hints:
    storage = PersistentDict(file=Path("/path/to/file.aks"))
    ```
    """

    def __init__(self, file: TextIOBase | Path) -> None:
        self._rwlock = RWLock()
        self._file: TextIOBase = self._get_file_object(file)

        try:
            self._data: T = json.load(self._file)
        except json.JSONDecodeError:
            # File is not empty, database exists but failed to load
            if self._file.read():
                raise Exception("Failed to load database.")

            # File is empty so it's a new database, make an empty dict
            self._data: T = {}

        # assert isinstance(self._data, dict), "Database file is not a dictionary"

    def _get_file_object(self, file) -> TextIOBase:
        if type(file) is Path:
            return open(file, "r+" if file.is_file() else "w+")
        elif isinstance(file, TextIOBase):
            return file
        else:
            raise Exception("Invalid file type")

    @ensure_file_is_open
    async def get(self, key: str) -> T[str, Any]:
        async with self._rwlock.reader_locked():
            # result =
            return self._data[key]

    @ensure_file_is_open
    async def set(self, key, value):
        async with self._rwlock.writer_locked():
            logging.debug(f"Persistent storage set {key} to {value}")

            self._data[key] = value

            self._file.truncate(0)
            self._file.seek(0)
            json.dump(self._data, self._file, sort_keys=True, indent=4)
            self._file.flush()

    @ensure_file_is_open
    async def remove(self, key):
        async with self._rwlock.writer_locked():
            logging.debug(f"Persistent storage removed {key}")
            self._data.pop(key)

            self._file.truncate(0)
            self._file.seek(0)
            json.dump(self._data, self._file, sort_keys=True, indent=4)
            self._file.flush()

    def close(self):
        if not self._file.closed:
            self._file.close()
            self._data = {}

    def __del__(self):
        """
        Ensure the file is closed when the object is destroyed.

        May have unexpected behaviour
        """
        self.close()
