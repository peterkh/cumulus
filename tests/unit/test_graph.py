from tests.unit import unittest
from cumulus.graph import StackDependencyGraph
from cumulus.exceptions import MutualDependencyError


class TestDependencyGraph(unittest.TestCase):

    def test_add_nodes(self):
        dg = StackDependencyGraph()
        dg.add_node('node1')
        dg.add_node('node2')
        dg.add_node('node3')
        self.assertItemsEqual(dg.nodes(), ['node1', 'node2', 'node3'])

    def test_double_add(self):
        dg = StackDependencyGraph()
        dg.add_node('node1')
        self.assertRaises(KeyError, dg.add_node, 'node1')

    def test_add_relations(self):
        dg = StackDependencyGraph()
        dg.add_node('node1')
        dg.add_node('node2')
        dg.add_node('node3')
        dg.add_dependency('node1', 'node2')
        dg.add_dependency('node2', 'node3')
        self.assertItemsEqual(dg.get_dependencies('node3'), ['node2'])
        self.assertItemsEqual(dg.get_successors('node1'), ['node2'])

    def test_add_relation_to_nonexistent(self):
        dg = StackDependencyGraph()
        dg.add_node('node1')
        dg.add_node('node2')
        self.assertRaises(KeyError, dg.add_dependency, 'node2', 'node3')
        self.assertRaises(KeyError, dg.add_dependency, 'node3', 'node2')

    def test_mutual_dependency(self):
        dg = StackDependencyGraph()
        dg.add_node('node1')
        dg.add_node('node2')
        dg.add_dependency('node1', 'node2')
        self.assertRaises(MutualDependencyError, dg.add_dependency, 'node2', 'node1')

    def test_delete_node(self):
        dg = StackDependencyGraph()
        dg.add_node('node1')
        dg.add_node('node2')
        dg.add_node('node3')
        dg.add_dependency('node1', 'node2')
        dg.add_dependency('node2', 'node3')
        dg.del_node('node2')
        self.assertItemsEqual(dg.get_dependencies('node3'), [])
        self.assertItemsEqual(dg.get_successors('node1'), [])
        self.assertItemsEqual(dg.nodes(), ['node1', 'node3'])

    def test_delete_nonexistent(self):
        dg = StackDependencyGraph()
        dg.add_node('node1')
        self.assertRaises(KeyError, dg.del_node, 'node2')

    def test_cycle_detection(self):
        import copy
        dg = StackDependencyGraph()
        dg.add_node('node1')
        dg.add_node('node2')
        dg.add_node('node3')
        dg.add_node('node4')
        dg1 = copy.deepcopy(dg)
        dg2 = copy.deepcopy(dg)
        dg3 = copy.deepcopy(dg)

        dg1.add_dependency('node1', 'node2')
        dg1.add_dependency('node2', 'node3')
        dg1.add_dependency('node3', 'node4')
        dg1.add_dependency('node4', 'node2')
        self.assertListEqual(dg1.find_cycle(), ['node2', 'node3', 'node4'])

        dg2.add_dependency('node1', 'node2')
        dg2.add_dependency('node2', 'node3')
        dg2.add_dependency('node3', 'node4')
        dg2.add_dependency('node4', 'node1')
        self.assertListEqual(dg2.find_cycle(), ['node1', 'node2', 'node3', 'node4'])

        dg3.add_dependency('node1', 'node2')
        dg3.add_dependency('node2', 'node3')
        dg3.add_dependency('node3', 'node4')
        self.assertListEqual(dg3.find_cycle(), [])

    def get_complex_graph(self):
        """
                       node1
                  _____/| \______
                 v      v        v
               node2   node3    node7
            ____/\___     \_    /
           v         v      v  v
         node4      node5  node6
          \____       |   __/
               \____  |  /
                    v v v
                    node8
        """
        dg = StackDependencyGraph()
        dg.add_node('node1')
        dg.add_node('node2')
        dg.add_node('node3')
        dg.add_node('node4')
        dg.add_node('node5')
        dg.add_node('node6')
        dg.add_node('node7')
        dg.add_node('node8')
        dg.add_dependency('node1', 'node2')
        dg.add_dependency('node1', 'node3')
        dg.add_dependency('node1', 'node7')
        dg.add_dependency('node2', 'node4')
        dg.add_dependency('node2', 'node5')
        dg.add_dependency('node3', 'node6')
        dg.add_dependency('node7', 'node6')
        dg.add_dependency('node4', 'node8')
        dg.add_dependency('node5', 'node8')
        dg.add_dependency('node6', 'node8')
        return dg

    def test_edge_detection(self):
        dg = self.get_complex_graph()

        self.assertItemsEqual(dg.get_edge_nodes(), ['node1'])
        dg.del_node('node1')
        self.assertItemsEqual(dg.get_edge_nodes(), ['node2', 'node3', 'node7'])
        dg.del_node('node2')
        self.assertItemsEqual(dg.get_edge_nodes(), ['node4', 'node5', 'node3', 'node7'])
        dg.del_node('node7')
        self.assertItemsEqual(dg.get_edge_nodes(), ['node4', 'node5', 'node3'])
        dg.del_node('node4')
        dg.del_node('node3')
        self.assertItemsEqual(dg.get_edge_nodes(), ['node5', 'node6'])
        dg.del_node('node5')
        dg.del_node('node6')
        self.assertItemsEqual(dg.get_edge_nodes(), ['node8'])
        dg.del_node('node8')
        self.assertItemsEqual(dg.get_edge_nodes(), [])

    def test_linear_traversal(self):
        dg = self.get_complex_graph()
        root = 'node1'
        expected_sequence = [1, 2, 4, 5, 3, 7, 6, 8]
        self.assertListEqual(list(dg.linear_traversal(root)), ['node'+str(i) for i in expected_sequence])