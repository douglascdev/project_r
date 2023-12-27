import asyncio
import json
import logging
from io import TextIOBase
from pathlib import Path
from typing import Any


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
        self._file = file

        self._r = asyncio.Lock()
        self._g = asyncio.Lock()
        self._b = 0

        with self._get_file_object() as file_object:
            try:
                self._data: dict = json.load(file_object)
            except json.JSONDecodeError:
                # File is not empty, database exists but failed to load
                if file_object.read():
                    raise Exception("Failed to load database.")

                # File is empty so it's a new database, make an empty dict
                self._data: dict = {}

            assert type(self._data) is dict, "Database file is not a dictionary"

    def _get_file_object(self) -> TextIOBase:
        if type(self._file) is Path:
            return open(self._file, "r+" if self._file.is_file() else "w+")
        elif isinstance(self._file, TextIOBase):
            return self._file
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

    async def get(self, key) -> Any:
        await self._begin_read()

        result = self._data.get(key, None)

        await self._end_read()

        return result

    async def set(self, key, value):
        await self._begin_write()

        logging.debug(f"Persistent storage set {key} to {value}")

        self._data[key] = value
        with self._get_file_object() as file_object:
            file_object.truncate(0)
            file_object.seek(0)
            json.dump(self._data, file_object, sort_keys=True, indent=4)

        await self._end_write()

    async def remove(self, key):
        await self._begin_write()

        logging.debug(f"Persistent storage removed {key}")
        self._data.pop(key)
        with self._get_file_object() as file_object:
            file_object.truncate(0)
            file_object.seek(0)
            json.dump(self._data, file_object, sort_keys=True, indent=4)

        await self._end_write()
