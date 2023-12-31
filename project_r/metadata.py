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
        """
        Returns the maximum space the node value can take.
        """
        return self.end_index - self.start_index + (1 if self.start_index == 0 else 0)


class _DisabledNodesList:
    """
    Manages operations on the list of disabled nodes.
    """

    def __init__(self) -> None:
        self.disabled_nodes: list[_ValueNode] = []

    def _bisect(self, min_size: int) -> int:
        return bisect_left(
            self.disabled_nodes, min_size, key=lambda node: node.available_space
        )

    def find_node(self, min_size: int) -> tuple[int, _ValueNode] | None:
        """
        Finds a node with available space greater than or equal to min_size using binary search,
        return its index and the node, or None if no disabled node fits the min_size requirement.
        """
        optimal_node_index = self._bisect(min_size)

        """
        If bisect returned an index that is out of bounds, no disabled node fits
        the min_size requirement and the caller will have to allocate a new node.
        """
        if optimal_node_index >= len(self.disabled_nodes):
            return None

        return optimal_node_index, self.disabled_nodes[optimal_node_index]

    def insert(self, node: _ValueNode) -> None:
        insort_left(self.disabled_nodes, node, key=lambda n: n.available_space)

    def remove(self, node: _ValueNode) -> None:
        """
        Remove node from disabled nodes list or raise ValueError if node is not found.
        """

        """
        Since nodes are sorted by available space, we can use binary search to
        find the leftmost node that would have the same available space, then use
        the next pointer to traverse the links until we find the correct node to remove.
        """
        initial_index = self._bisect(node.available_space)
        num_nodes_traversed = 0

        current_node = self.disabled_nodes[initial_index]
        while (
            current_node not in (None, node)
            and current_node.available_space == node.available_space
        ):
            current_node = current_node.next_node
            num_nodes_traversed += 1

        if current_node is node:
            self.disabled_nodes.pop(initial_index + num_nodes_traversed)
        else:
            raise ValueError(f"Node {node} not in disabled nodes.")

    def pop(self, index: int) -> _ValueNode:
        return self.disabled_nodes.pop(index)


class _Metadata:
    """
    Metadata that is written to disk.
    """

    def __init__(self) -> None:
        self.file_size: int = 0
        # Maps database keys to their corresponding database nodes
        self.key_to_node: dict[str, _ValueNode] = {}
        self.disabled_nodes: list[_ValueNode] = []
        self.last_node: _ValueNode | None = None

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

        self._disabled_nodes_list = _DisabledNodesList()

    def _add_new_node(self, key: str, value_size: int):
        # Try allocating in a disabled node that fits the value size
        # TODO: if the disabled node has more space than needed, split it in two,
        #       creating a new disabled node with the remaining space
        index_and_node = self._disabled_nodes_list.find_node(min_size=value_size)
        if index_and_node is not None:
            index, node = index_and_node
            self._disabled_nodes_list.pop(index)
            self._metadata.key_to_node[key] = node
            node.value_size = value_size
            node.is_enabled = True

        elif self._metadata.last_node is None:
            # First node
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
            node.previous_node = self._metadata.last_node
            self._metadata.last_node = node
            self._metadata.file_size += value_size
            self._metadata.key_to_node[key] = self.last_node

    def _disable_node(self, node: _ValueNode):
        """
        Disable node and try to traverse the list of disabled nodes to its left and right,
        turning the sequence of disabled nodes into a new single node.
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
