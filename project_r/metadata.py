import json
from bisect import bisect_left, insort_left
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path


@dataclass
class _ValueNode:
    """
    Keeps all information about a node stored in the database file, such as
    the starting and ending index of the value.
    """

    start_index: int
    end_index: int
    value_size: int
    previous_node: "None | _ValueNode"
    next_node: "None | _ValueNode"
    is_enabled: bool = True

    @property
    def available_space(self) -> int:
        return self.end_index - self.start_index


class _DisabledNodes:
    """
    Uses binary search to find and insert nodes into the disabled node list.
    """

    def __init__(self) -> None:
        self.disabled_nodes: list[_ValueNode] = []

    def find_node(self, min_size: int) -> _ValueNode | None:
        """
        Finds a node with available space greater than or equal to min_size using binary search.
        """
        optimal_node_index = bisect_left(
            self.disabled_nodes, min_size, key=lambda node: node.available_space
        )
        if self.disabled_nodes[optimal_node_index].available_space >= min_size:
            return self.disabled_nodes[optimal_node_index]
        else:
            return None

    def insert(self, node: _ValueNode) -> None:
        insort_left(self.disabled_nodes, node, key=lambda n: n.available_space)

    def remove(self, node: _ValueNode) -> None:
        self.disabled_nodes.remove(node)


@dataclass
class _Metadata:
    """
    Metadata that is written to disk.
    """

    file_size: int = 0
    # Maps database keys to their corresponding database nodes
    key_to_node: dict[str, _ValueNode] = {}
    disabled_nodes: list[_ValueNode] = []
    last_node: _ValueNode | None = None

    def toJSON(self) -> str:
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


class _OperationType(Enum):
    SET_VALUE = auto()
    ADD_NEW = auto()
    RELOCATE = auto()
    REMOVE = auto()


class MetadataController:
    """
    Handles operations on database metadata.
    """

    def __init__(self, db_path: Path) -> None:
        # Use same name as database file, but remove extension and add "_metadata.json"
        self._file = Path(db_path.stem) / "_metadata.json"

        # TODO: load from json
        self._metadata = _Metadata()

    def _add_new_node(self, key: str, value_size: int):
        """
        TODO:
         - try allocating on a disabled node that fits the value size
        """
        if self._metadata.last_node is None:
            self.last_node = _ValueNode(
                start_index=0,
                end_index=value_size,
                value_size=value_size,
                previous_node=self._metadata.last_node,
                next_node=None,
            )
            self._metadata.file_size += value_size
            self._metadata.key_to_node[key] = self.last_node
        else:
            node = _ValueNode(
                start_index=self._metadata.file_size,
                end_index=self._metadata.file_size + value_size,
                value_size=value_size,
                previous_node=self._metadata.last_node,
                next_node=None,
            )
            self._metadata.last_node.next_node = node
            self._metadata.last_node = node
            self._metadata.file_size += value_size
            self._metadata.key_to_node[key] = self.last_node

    def _disable_node(self, node: _ValueNode):
        """
        Disables node and tries to find a sequence of previous disabled nodes that can be combined to free more space.
        """
        node.is_enabled = False
        # TODO

    def set(self, key: str, value_size: int) -> tuple[int, int]:
        if key in self._metadata.key_to_node:
            if value_size > self._metadata.key_to_node[key].available_space:
                op_type = _OperationType.RELOCATE
            else:
                op_type = _OperationType.SET_VALUE
        else:
            op_type = _OperationType.ADD_NEW

        match op_type:
            case _OperationType.SET_VALUE:
                self._metadata.key_to_node[key].value_size = value_size
            case _OperationType.ADD_NEW:
                self._add_new_node(key, value_size)
            case _OperationType.RELOCATE:
                self._disable_node(self._metadata.key_to_node[key])
                self._add_new_node(key, value_size)

    def get(self, key: str) -> tuple[int, int] | None:
        """
        Find tuple with indexes of where a value starts and ends in the file
        """
        if node := self._metadata.key_to_node[key]:
            return node.start_index, node.start_index + node.value_size
        else:
            return None

    def remove(self, key: str) -> None:
        node = self._metadata.key_to_node.pop(key)
        self._disable_node(node)
        # TODO: add node to disabled node map
