
class CumulusException(Exception):
    """
    Base class for custom exceptions raised by cumulus
    """
    pass


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
    def __init__(self, node1, node2):
        self.node1 = node1
        self.node2 = node2

    def __repr__(self):
        return "Mutual dependency between %s and %s" % (self.node1, self.node2)


class DependencyLoopError(DependencyGraphError):
    def __init__(self, loopnodes):
        self.loopnodes = loopnodes

    def __repr__(self):
        return "Dependency loop detected: %s -> %s" % (" -> ".join(self.loopnodes), self.loopnodes[0])