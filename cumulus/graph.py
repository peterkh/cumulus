from sys import getrecursionlimit, setrecursionlimit
from .exceptions import MutualDependencyError


class StackDependencyGraph:
    def __init__(self):
        self.node_deps = {}
        self.node_successors = {}

    def add_node(self, node):
        """
        Add a new node to the graph
        @param node: The new node to add in the graph
        @type node: str
        @raise KeyError: If node is already present in the graph
        """
        if node not in self.node_deps and node not in self.node_successors:
            self.node_deps[node] = []
            self.node_successors[node] = []
        else:
            raise KeyError("Node %s already present in the graph" % node)

    def add_dependency(self, parent, children):
        """
        Add a new dependency (edge) between two nodes
        @param parent: The parent in the relation between the two nodes
        @type parent: str
        @param children: The children in the relation between the two nodes
        @type children: str
        @raise KeyError: If either parent or children are not present in the graph node list
        """
        if parent in self.node_deps and parent in self.node_successors:
            if children in self.node_deps and children in self.node_successors:
                if parent not in self.node_successors[children]:
                    self.node_successors[parent].append(children)
                    self.node_deps[children].append(parent)
                else:
                    raise MutualDependencyError(parent, children)
            else:
                raise KeyError("Node %s not present in graph" % children)
        else:
            raise KeyError("Node %s not present in graph" % parent)

    def del_node(self, node):
        """
        Delete a node from the graph and clear its relations to others
        @param node: The node to delete
        @raise ManipulationError: If node can't be found in the graph
        """
        if node in self.node_deps and node in self.node_successors:
            for successor in self.node_successors[node]:
                self.node_deps[successor].remove(node)
            del self.node_successors[node]
            for dep in self.node_deps[node]:
                self.node_successors[dep].remove(node)
            del self.node_deps[node]
        else:
            raise KeyError("Node %s not present in graph" % node)

    def nodes(self):
        """
        List nodes currently
        @return: A list of nodes in the graph
        @rtype: list
        """
        return list(self.node_deps.keys())

    def get_dependencies(self, node):
        """
        Return a list of dependencies for the given node
        @param node: Input node
        @type node: str
        @return: A list of dependencies
        @rtype: list
        @raise KeyError: If the selected node cannot be found
        """
        if node in self.node_deps:
            return self.node_deps[node]
        else:
            raise KeyError("Node %s not present in graph" % node)

    def get_successors(self, node):
        """
        Return a list of successors for the given node
        @param node: Input node
        @type node: str
        @return: A list of successors
        @rtype: list
        @raise KeyError: If the selected node cannot be found
        """
        if node in self.node_successors:
            return self.node_successors[node]
        else:
            raise KeyError("Node %s not present in graph" % node)

    def find_cycle(self):
        """
        Find loops in the current graph. Returns a list of nodes forming a cycle or an empty list if none is found
        @return: A list of nodes creating a loop in the graph or empty list if none is found
        @rtype: list
        """
        graph = self.node_successors

        def find_cycle_to_ancestor(node, ancestor):
            """
            Find a cycle containing both node and ancestor.
            """
            path = []
            while node != ancestor:
                if node is None:
                    return []
                path.append(node)
                node = spanning_tree[node]
            path.append(node)
            path.reverse()
            return path

        def dfs(node):
            """
            Depth-first search subfunction.
            """
            visited[node] = 1
            # Explore recursively the connected component
            for each in graph[node]:
                if cycle:
                    return
                if each not in visited:
                    spanning_tree[each] = node
                    dfs(each)
                else:
                    if spanning_tree[node] != each:
                        cycle.extend(find_cycle_to_ancestor(node, each))

        recursionlimit = getrecursionlimit()
        setrecursionlimit(max(len(self.nodes()) * 2, recursionlimit))

        visited = {}              # List for marking visited and non-visited nodes
        spanning_tree = {}        # Spanning tree
        cycle = []

        # Algorithm outer-loop
        for each in graph:
            # Select a non-visited node
            if each not in visited:
                spanning_tree[each] = None
                # Explore node's connected component
                dfs(each)
                if cycle:
                    setrecursionlimit(recursionlimit)
                    return cycle

        setrecursionlimit(recursionlimit)
        return []

    def get_edge_nodes(self):
        """
        Get a list of nodes with no dependencies
        @return: A list of nodes
        @rtype: list
        """
        edge_nodes = []
        for node_dep in self.node_deps:
            if len(self.node_deps[node_dep]) == 0:
                edge_nodes.append(node_dep)
        return edge_nodes

    def linear_traversal(self, node=None):
        """
        Graph linear traversal iterator.

        @param node: Root node for graph traversal.
        @type  node: str
        @rtype:  iterator
        @return: Traversal iterator.
        """
        def _dfs(visited, node):
            """
            Dependency-safe Depth-first search subfunction.
            """
            if not node and len(self.nodes()):
                node = self.nodes()[0]
            visited[node] = 1
            yield node
            # Explore recursively the connected component
            for successor in self.node_successors[node]:
                deps = self.node_deps[successor]
                if successor not in visited and all([dep in visited for dep in deps]):
                    for other in _dfs(visited, successor):
                        yield other

        visited = {}
        for each in _dfs(visited, node):
            yield each