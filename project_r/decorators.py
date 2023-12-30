from project_r.exceptions import DatabaseClosedError


def ensure_file_is_open(func):
    async def wrapper(*args):
        self = args[0]
        if self._file.closed:
            raise DatabaseClosedError()

        return await func(*args)

    return wrapper
