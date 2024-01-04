import unittest
from io import StringIO
from pathlib import Path
from typing import TypedDict

from project_r import DatabaseClosedError, PersistentDict


class MyData(TypedDict):
    asd: str
    age: int


class PersistentDictBasicTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.buffer = StringIO("")
        self.storage = PersistentDict[MyData](self.buffer)

    async def test_get_set_remove(self):
        storage = self.storage
        await storage.set("name", "ASD")
        asd = await storage.get("asd")
        storage._data["asd"]
        # name.

        await self.storage.set("asd", "asd")
        self.assertEqual(await self.storage.get("asd"), "asd")
        asd = await self.storage.get("asd")
        await self.storage.remove("asd")
        self.assertIs(await self.storage.get("asd"), None)

    async def test_get_set_remove_after_close(self):
        self.storage.close()

        with self.assertRaises(DatabaseClosedError):
            await self.storage.set("asd", "asd")

        with self.assertRaises(DatabaseClosedError):
            self.assertEqual(await self.storage.get("asd"), "asd")

        with self.assertRaises(DatabaseClosedError):
            await self.storage.remove("asd")


if __name__ == "__main__":
    unittest.main()
