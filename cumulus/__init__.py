#!/usr/bin/env python
# Peter Hall 20/03/2013
#
# Take a yaml file that describes a full VPC made of multiple CF templates

import argparse
import logging
import time
from boto import cloudformation
from MegaStack import MegaStack, StackDependencyGraph, DependencyError
import sys
from pygraph.algorithms.sorting import topological_sorting
from pygraph.algorithms.accessibility import *
from pygraph.algorithms.searching import *
import networkx
import pylab as p
import uuid






def main():

    conf_parser = argparse.ArgumentParser()
    conf_parser.add_argument("-y", "--yamlfile", dest="yamlfile", required=True, help="The yaml file to read the VPC mega stack configuration from")
    conf_parser.add_argument("-a", "--action", dest="action", required=True, help="The action to preform: create, check, update, delete or watch")
    conf_parser.add_argument("-l", "--log", dest="loglevel", required=False, default="info", help="Log Level for output messages, CRITICAL, ERROR, WARNING, INFO or DEBUG")
    conf_parser.add_argument("-L", "--botolog", dest="botologlevel", required=False, default="critical", help="Log Level for boto, CRITICAL, ERROR, WARNING, INFO or DEBUG")
    conf_parser.add_argument("-s", "--stack", dest="stackname", required=False, help="The stack name, used with the watch action, ignored for other actions")
    args = conf_parser.parse_args()

    #Validate that action is something we know what to do with
    valid_actions = ['create', 'check', 'update', 'delete', 'watch']
    if args.action not in valid_actions:
        print "Invalid action provided, must be one of: '%s'" % ( ", ".join(valid_actions) )
        exit(1)

    #Make sure we can read the yaml file provided
    try:
        readable = open(args.yamlfile, 'r')
    except IOError as e:
        print "Cannot read yaml file %s: %s" % (args.yamlfile, e)
        exit(1)

    #Get and configure the log level
    numeric_level = getattr(logging, args.loglevel.upper(), None)
    boto_numeric_level = getattr(logging, args.botologlevel.upper(), None)
    if not isinstance(numeric_level, int):
        print 'Invalid log level: %s' % loglevel
        exit(1)
    logging.basicConfig(level=numeric_level)
    logger = logging.getLogger(__name__)

    #Get and configure the log level for boto
    if not isinstance(boto_numeric_level, int):
        logger.critical("Invalid boto log level: %s" % botologlevel)
        exit(1)
    logging.getLogger('boto').setLevel(boto_numeric_level)

    #Create the mega_stack object and sort out dependencies
    the_mega_stack = MegaStack(args.yamlfile)
    try:
      the_mega_stack.build_dep_graph()
    except DependencyError,e :
      logger.critical(repr(e))

    # def get_relations(root):
    #   successors = networkx.dfs_successors(the_mega_stack.dep_graph, root)
    #   predecessors = networkx.dfs_predecessors(the_mega_stack.dep_graph, root)
    #   print "Predecessors of %s = %s" % (root, predecessors)
    #   print "Successors of %s = %s" % (root, successors)
    #   return (predecessors, successors)
    #
    #
    # order = networkx.topological_sort(the_mega_stack.dep_graph)
    # for item in order:
    #   pre,suc  = get_relations(item)
    #   try:
    #     pre_req = pre[item]
    #   except KeyError:
    #
    #     print "%s has no pre-requisites, then creating...." % item


    #print the_mega_stack.dep_graph.node_deps
    #print the_mega_stack.dep_graph.node_successors
    print the_mega_stack.dep_graph.get_edge_nodes()
    print "capitol has been created"
    the_mega_stack.dep_graph.del_node('capitol')
    print the_mega_stack.dep_graph.get_edge_nodes()
    print "mng-sub has been created"
    the_mega_stack.dep_graph.del_node('mng-sub')
    print the_mega_stack.dep_graph.get_edge_nodes()
    print "fe-sub has been created"
    the_mega_stack.dep_graph.del_node('fe-sub')
    print the_mega_stack.dep_graph.get_edge_nodes()

    print "mng-sslvpn has been created"
    the_mega_stack.dep_graph.del_node('mng-sslvpn')
    print the_mega_stack.dep_graph.get_edge_nodes()

    print "be-sub has been created"
    the_mega_stack.dep_graph.del_node('be-sub')
    print the_mega_stack.dep_graph.get_edge_nodes()

    print "pupmaster has been created"
    the_mega_stack.dep_graph.del_node('pupmaster')
    print the_mega_stack.dep_graph.get_edge_nodes()

    print "database has been created"
    the_mega_stack.dep_graph.del_node('database')
    print the_mega_stack.dep_graph.get_edge_nodes()

    #spacer = {the_mega_stack.name: 0}
    #for prereq, target in networkx.dfs_edges(the_mega_stack.dep_graph, the_mega_stack.name):
    #  spacer[target] = spacer[prereq] + 2
    #  print '{spacer}+-{t}'.format(spacer=' ' * spacer[prereq],  t=target)
    #networkx.draw(the_mega_stack.dep_graph)
    #p.show()
    #st, order = breadth_first_search(the_mega_stack.dep_graph, root=the_mega_stack.name)
    #print st
    #print order
    # order = networkx.topological_sort(the_mega_stack.dep_graph)
    # print order
    #
    # for item in order:
    #   successors = networkx.dfs_successors(the_mega_stack.dep_graph, item)
    #   if item in successors:
    #     print "Item is free to go"
    #
    #
    # start = order[0]
    # nodes = [order[0]]
    # labels = {}
    # print "edges"
    #
    #
    #
    # tree = networkx.Graph()
    # while nodes:
    #   source = nodes.pop()
    #   print "Neighbors of %s : %s" % (source, the_mega_stack.dep_graph.neighbors(source))
    #   print "Predecessors of %s : %s" % (source, networkx.dfs_predecessors(the_mega_stack.dep_graph, source))
    #   print "Successors of %s : %s" % (source, networkx.dfs_successors(the_mega_stack.dep_graph, source))
    #   #print the_mega_stack.dep_graph.
    #   for target in the_mega_stack.dep_graph.neighbors(source):
    #     nodes.append(target)
    # networkx.draw(the_mega_stack.dep_graph)
    # p.show()

    #Print some info about what we found in the yaml and dependency order
    #logger.info("Mega stack name: %s" % the_mega_stack.name)
    #logger.info("Found %s CF stacks in yaml." % len(the_mega_stack.cf_stacks))
    #logger.info("Processing stacks in the following order: %s" % [x.name for x in the_mega_stack.stack_objs])
    #for stack in the_mega_stack.stack_objs:
    #    logger.debug("%s depends on %s" % (stack.name, stack.depends_on))

    sys.exit(0)
    #Run the method of the mega stack object for the action provided
    if args.action == 'create':
        the_mega_stack.create()

    if args.action == 'check':
        the_mega_stack.check()

    if args.action == 'delete':
        the_mega_stack.delete()

    if args.action == 'update':
        the_mega_stack.update()

    if args.action == 'watch':
        the_mega_stack.watch(args.stackname)

if __name__ == '__main__':
    main()
