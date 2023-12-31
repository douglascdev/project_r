import unittest

from project_r.metadata import _DisabledNodesList, _ValueNode


class TestMetadataDisabledNodes(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.disabled_nodes_list = _DisabledNodesList()

        # Available sizes: 1, 2, 2, 3, 5, 10
        nodes = [
            _ValueNode(0, 2, 0, None, None),
            _ValueNode(3, 4, 0, None, None),
            _ValueNode(5, 7, 0, None, None),
            _ValueNode(8, 10, 0, None, None),
            _ValueNode(11, 16, 0, None, None),
            _ValueNode(17, 27, 0, None, None),
        ]

        for node in nodes:
            self.disabled_nodes_list.insert(node)

        # Link nodes(used to test node removal)
        nodes[0].next_node = nodes[1]
        nodes[-1].previous_node = nodes[-2]
        for i, node in enumerate(nodes[1:-1], start=1):
            node.next_node = nodes[i + 1]
            node.previous_node = nodes[i - 1]

    async def test_inserted_order(self):
        # Nodes are expected to be sorted by available space: 1, 2, 2, 3, 5, 10
        available_space_list = [
            node.available_space for node in self.disabled_nodes_list.disabled_nodes
        ]
        self.assertEqual(available_space_list, [1, 2, 2, 3, 5, 10])

    async def test_find_node(self):
        node_minsize_to_expected_index = {
            0: 0,
            1: 0,
            2: 1,
            3: 3,
            4: 4,
            5: 4,
            6: 5,
            7: 5,
            8: 5,
            9: 5,
            10: 5,
        }
        for node_size, expected_index in node_minsize_to_expected_index.items():
            result = self.disabled_nodes_list.find_node(node_size)
            expected_node = self.disabled_nodes_list.disabled_nodes[expected_index]
            self.assertEqual(
                result,
                expected_node,
                f"Failed for {node_size}. Expected node {expected_node} but got {result}.",
            )

        # Test nodes that would not fit into existing disabled nodes
        assert self.disabled_nodes_list.find_node(11) is None
        assert self.disabled_nodes_list.find_node(100) is None

    async def test_remove(self):
        first_node = self.disabled_nodes_list.disabled_nodes[0]
        self.disabled_nodes_list.remove(first_node)
        available_space_list = [
            node.available_space for node in self.disabled_nodes_list.disabled_nodes
        ]
        # 1, 2, 2, 3, 5, 10 => 2, 2, 3, 5, 10
        self.assertEqual(
            available_space_list,
            [2, 2, 3, 5, 10],
        )

        # Try removing a node that is not in the list
        with self.assertRaises(ValueError):
            self.disabled_nodes_list.remove(first_node)

        # Try removing node with available space 3 from the middle of the list, check if the correct order is maintained
        # 2, 2, 3, 5, 10 => 2, 2, 5, 10
        middle_node = self.disabled_nodes_list.disabled_nodes[2]
        self.disabled_nodes_list.remove(middle_node)
        available_space_list = [
            node.available_space for node in self.disabled_nodes_list.disabled_nodes
        ]
        self.assertEqual(available_space_list, [2, 2, 5, 10])

        # Try removing node with available space 10 from the end of the list, check if the correct order is maintained
        end_node = self.disabled_nodes_list.disabled_nodes[-1]
        self.disabled_nodes_list.remove(end_node)
        available_space_list = [
            node.available_space for node in self.disabled_nodes_list.disabled_nodes
        ]
        # [2, 2, 5, 10] => [2, 2, 5]
        self.assertEqual(available_space_list, [2, 2, 5])


if __name__ == "__main__":
    unittest.main()
