import unittest

from project_r.metadata import _DisabledNodesList, _ValueNode


class TestMetadataDisabledNodes(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.disabled_nodes_list = _DisabledNodesList()

        # Available space: 3, 1, 2, 1, 5, 10
        self.nodes = nodes = [
            _ValueNode(0, 2, 0, None, None, False),  #  3
            _ValueNode(3, 4, 0, None, None, False),  #  1
            _ValueNode(5, 7, 0, None, None, False),  #  2
            _ValueNode(8, 10, 0, None, None, False),  #  2
            _ValueNode(11, 16, 0, None, None, False),  #  5
            _ValueNode(17, 27, 0, None, None, False),  # 10
        ]

        # Doesn't trigger defragmentation, since they're not linked yet
        for node in nodes:
            self.disabled_nodes_list.insert(node)

        # Link nodes(used to test node removal and defragmentation)
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
            i, result = self.disabled_nodes_list.find_node(node_size)
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

    async def test_node_defragmentation_middle(self):
        """
        Tests defragmentation for an element inserted in the middle.

        Defragmentation means that inserting nodes with nearby disabled nodes makes them all combine into the new node,
        freeing up more space for allocating nodes.
        """
        # Since nodes are linked by the time this insertion runs, it'll trigger defragmentation
        node_1 = self.disabled_nodes_list.disabled_nodes[1]
        node = _ValueNode(4, 4, 0, node_1, node_1.next_node, False)
        node_1.next_node.previous_node = node
        node_1.next_node = node
        self.disabled_nodes_list.insert(node)

        # Defragmentation is expected to turn the whole list into the a new node
        self.assertEqual(len(self.disabled_nodes_list.disabled_nodes), 1)
        combined_node = self.disabled_nodes_list.disabled_nodes[0]
        self.assertEqual(combined_node.start_index, 0)
        self.assertEqual(combined_node.end_index, 27)
        self.assertEqual(combined_node.available_space, 28)

    async def test_node_defragmentation_end(self):
        """
        Tests defragmentation for an element inserted at the end.
        """
        # Since nodes are linked by the time this insertion runs, it'll trigger defragmentation
        last_node = self.disabled_nodes_list.disabled_nodes[-1]
        node = _ValueNode(28, 48, 0, last_node, None, False)
        self.disabled_nodes_list.insert(node)

        # Defragmentation is expected to turn the whole list into the a new node
        self.assertEqual(len(self.disabled_nodes_list.disabled_nodes), 1)
        combined_node = self.disabled_nodes_list.disabled_nodes[0]
        self.assertEqual(combined_node.start_index, 0)
        self.assertEqual(combined_node.end_index, 48)
        self.assertEqual(combined_node.available_space, 49)

    async def test_node_defragmentation_start(self):
        """
        Tests defragmentation for an element inserted at the start.
        """
        # Since nodes are linked by the time this insertion runs, it'll trigger defragmentation
        first_node = self.nodes[0]
        node = _ValueNode(0, 0, 0, None, first_node, False)
        first_node.previous_node = node
        self.disabled_nodes_list.insert(node)

        # Defragmentation is expected to turn the whole list into the a new node
        self.assertEqual(len(self.disabled_nodes_list.disabled_nodes), 1)
        combined_node = self.disabled_nodes_list.disabled_nodes[0]
        self.assertEqual(combined_node.start_index, 0)
        self.assertEqual(combined_node.end_index, 27)
        self.assertEqual(combined_node.available_space, 28)


if __name__ == "__main__":
    unittest.main()
