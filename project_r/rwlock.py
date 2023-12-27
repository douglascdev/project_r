from asyncio import Lock
from typing import AsyncContextManager


class WriteLockContextManager:
    def __init__(self, rwlock: "RWLock") -> None:
        self.rwlock = rwlock

    async def __aenter__(self):
        await self.rwlock._begin_write()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.rwlock._end_write()


class ReadLockContextManager:
    def __init__(self, rwlock) -> None:
        self.rwlock = rwlock

    async def __aenter__(self):
        await self.rwlock._begin_read()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.rwlock._end_read()


class RWLock:
    """
    Lock implemented based on wikipedia's pseucode for a Readers–writer lock
    """

    def __init__(self):
        self._r = Lock()
        self._g = Lock()
        self._b = 0

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

    def writer_locked(self) -> AsyncContextManager[WriteLockContextManager]:
        return WriteLockContextManager(self)

    def reader_locked(self) -> AsyncContextManager[ReadLockContextManager]:
        return ReadLockContextManager(self)
