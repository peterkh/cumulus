
class CumulusException(Exception):
    """
    Base class for custom exceptions raised by cumulus
    """
    pass

class StackStatusInconsistent(CumulusException):
    """
    This exception is raised when a stack is found in an inconsistent or unexpected state
    """

class DependencyGraphError(CumulusException):
    """
    Base class for exceptions occurring in the scope of the dependency graph
    """
    pass


class ManipulationError(DependencyGraphError):
    """
    This exception is raised when a add/remove node operation fails
    """
    pass

class LookupError(DependencyGraphError):
    """
    This exception is raised when a selected node cannot be found in the graph
    """
    pass


class MutualDependencyError(DependencyGraphError):
    pass
    def __init__(self, node1, node2):
        self.args = [node1, node2]

    def __str__(self):
        return "Mutual dependency between %s and %s" % (self.args[0], self.args[1])


class DependencyLoopError(DependencyGraphError):
    def __init__(self, loopnodes):
        self.args = [loopnodes]

    def __str__(self):
        nodes = self.args[0]
        return "Dependency loop detected: %s -> %s" % (" -> ".join(nodes), nodes[0])