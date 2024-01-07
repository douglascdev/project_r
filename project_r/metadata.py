import json
from bisect import bisect_left
from dataclasses import dataclass
from enum import Enum, auto
from io import TextIOBase


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
        return (
            self.end_index
            - self.start_index
            + (1 if self.start_index == 0 != self.end_index else 0)
        )


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

    def _index(self, node: _ValueNode) -> int | None:
        index_and_node = self.find_node(node.available_space)
        if index_and_node is None:
            return None

        i, found_node = index_and_node
        while (
            found_node not in (None, node)
            and found_node.available_space == node.available_space
            and i < len(self.disabled_nodes)
        ):
            i += 1
            found_node = self.disabled_nodes[i]

        return i if found_node is node else None

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
        """
        Insert node into disabled nodes list.

        Any nodes to its left or right that are also disabled get combined into it.
        """
        index = self._bisect(node.available_space)
        self.disabled_nodes.insert(index, node)

        left_edge = right_edge = node
        combinable_node_indexes = set()

        # Traverse nodes left and right to find nodes that can be combined
        while (
            left_edge.previous_node is not None
            and left_edge.previous_node.is_enabled is False
        ):
            left_edge = left_edge.previous_node
            found_node_i = self._index(left_edge)
            if found_node_i is not None:
                combinable_node_indexes.add(found_node_i)

        while (
            right_edge.next_node is not None
            and right_edge.next_node.is_enabled is False
        ):
            right_edge = right_edge.next_node
            found_node_i = self._index(right_edge)
            if found_node_i is not None:
                combinable_node_indexes.add(found_node_i)

        # Fix links to match the new status of one combined node
        if left_edge is not node:
            if left_edge.previous_node:
                left_edge.previous_node.next_node = node

            node.previous_node = left_edge.previous_node
            node.start_index = left_edge.start_index

        if right_edge is not node:
            if right_edge.next_node:
                right_edge.next_node.previous_node = node

            node.next_node = right_edge.next_node
            node.end_index = right_edge.end_index

        # Remove combined nodes from disabled nodes list
        self.disabled_nodes = [
            node
            for i, node in enumerate(self.disabled_nodes)
            if i not in combinable_node_indexes
        ]

    def remove(self, node: _ValueNode) -> None:
        """
        Remove node from disabled nodes list or raise ValueError if node is not found.
        """

        """
        Since nodes are sorted by available space, we can use binary search to
        find the leftmost node that would have the same available space, then use
        the next pointer to traverse the links until we find the correct node to remove.
        """
        node_index = self._index(node)

        if node_index is None:
            raise ValueError(f"Node {node} not in disabled nodes.")

        self.disabled_nodes.pop(node_index)

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

    def load(self, metadata_io: TextIOBase) -> None:
        try:
            data: dict = json.load(metadata_io)
        except json.JSONDecodeError:
            # File is not empty, database exists but failed to load
            if metadata_io.read():
                raise Exception("Failed to load database.")

            # File is empty so it's a new database, don't have to load anything
            return

        self.__dict__.update(data)

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

    def __init__(self, metadata_io: TextIOBase) -> None:
        # Use same name as database file, but remove extension and add "_metadata.json"
        self._metadata_io = metadata_io

        self._metadata = _Metadata()
        self._metadata.load(self._metadata_io)

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
        Disable node. Combine with any adjacent disabled nodes.
        """
        node.is_enabled = False
        self._disabled_nodes_list.insert(node)

    def set(self, key: str, value_size: int) -> None:
        """
        Update the allocated character index range in the file for the
        value according to its size.
        """
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
