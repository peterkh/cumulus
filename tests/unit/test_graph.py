from tests.unit import unittest
from cumulus.graph import StackDependencyGraph

class TestDependencyGraph(unittest.TestCase):

    def test_add_nodes(self):
        dg = StackDependencyGraph()
        dg.add_node('node1')
        dg.add_node('node2')
        dg.add_node('node3')
        self.assertListEqual(sorted(dg.nodes()), sorted(['node1', 'node2', 'node3']))

    def test_add_relations(self):
        dg = StackDependencyGraph()
        dg.add_node('node1')
        dg.add_node('node2')
        dg.add_node('node3')
        dg.add_dependency('node1', 'node2')
        dg.add_dependency('node2', 'node3')
        print dg.node_deps
        print dg.node_successors
        self.assertListEqual(sorted(dg.get_dependencies('node3')), ['node2'])
        self.assertListEqual(sorted(dg.get_successors('node1')), ['node2'])
