class DatabaseClosedError(Exception):
    def __init__(self) -> None:
        super().__init__("Database file object is closed.")


