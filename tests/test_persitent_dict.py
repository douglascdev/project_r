import unittest
from io import StringIO

from project_r import DatabaseClosedError, PersistentDict


class PersistentDictBasicTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.buffer = StringIO("")
        self.storage = PersistentDict(self.buffer)

    async def test_get_set_remove(self):
        await self.storage.set("asd", "asd")
        self.assertEqual(await self.storage.get("asd"), "asd")
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
