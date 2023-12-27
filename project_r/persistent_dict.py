import asyncio
import json
import logging
from io import TextIOBase
from pathlib import Path
from typing import Any

__all__ = ["PersistentDict", "DatabaseClosedError"]


class DatabaseClosedError(Exception):
    def __init__(self) -> None:
        super().__init__("Database file object is closed.")


def ensure_file_is_open(func):
    async def wrapper(*args):
        self = args[0]
        if self._is_file_closed:
            raise DatabaseClosedError()

        return await func(*args)

    return wrapper


class PersistentDict:
    """
    Persistent async key-value storage.

    Locks implemented based on wikipedia's pseucode for a Readers–writer lock
    """

    def __init__(self, file: TextIOBase | Path) -> None:
        """
        File can be a Path to a database file or a TextIOBase if you don't want to use a file,
        such as using a StringIO object for a memory database.
        """
        self._r = asyncio.Lock()
        self._g = asyncio.Lock()
        self._b = 0

        self._file: TextIOBase = self._get_file_object(file)
        self._is_file_closed = False

        try:
            self._data: dict = json.load(self._file)
        except json.JSONDecodeError:
            # File is not empty, database exists but failed to load
            if self._file.read():
                raise Exception("Failed to load database.")

            # File is empty so it's a new database, make an empty dict
            self._data: dict = {}

        assert type(self._data) is dict, "Database file is not a dictionary"

    def _get_file_object(self, file) -> TextIOBase:
        if type(file) is Path:
            return open(file, "r+" if file.is_file() else "w+")
        elif isinstance(file, TextIOBase):
            return file
        else:
            raise Exception("Invalid file type")

    async def _begin_read(self):
        await self._r.acquire()
        self._b += 1
        if self._b == 1:
            await self._g.acquire()
        self._r.release()

    async def _end_read(self):
        await self._r.acquire()
        self._b -= 1
        if self._b == 0:
            self._g.release()
        self._r.release()

    async def _begin_write(self):
        await self._g.acquire()

    async def _end_write(self):
        self._g.release()

    @ensure_file_is_open
    async def get(self, key) -> Any:
        await self._begin_read()

        result = self._data.get(key, None)

        await self._end_read()

        return result

    @ensure_file_is_open
    async def set(self, key, value):
        await self._begin_write()

        logging.debug(f"Persistent storage set {key} to {value}")

        self._data[key] = value

        self._file.truncate(0)
        self._file.seek(0)
        json.dump(self._data, self._file, sort_keys=True, indent=4)
        self._file.flush()

        await self._end_write()

    @ensure_file_is_open
    async def remove(self, key):
        await self._begin_write()

        logging.debug(f"Persistent storage removed {key}")
        self._data.pop(key)

        self._file.truncate(0)
        self._file.seek(0)
        json.dump(self._data, self._file, sort_keys=True, indent=4)
        self._file.flush()

        await self._end_write()

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
