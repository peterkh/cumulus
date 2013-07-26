#!/usr/bin/env python
# Peter Hall 20/03/2013
#
# Take a yaml file that describes a full VPC made of multiple CF templates

import argparse
import logging
import time
from boto import cloudformation
from MegaStack import MegaStack

from .exceptions import CumulusException

def main():

    conf_parser = argparse.ArgumentParser()
    conf_parser.add_argument("-y", "--yamlfile", dest="yamlfile", required=True, help="The yaml file to read the VPC mega stack configuration from")
    conf_parser.add_argument("-a", "--action", dest="action", required=True, help="The action to preform: create, check, update, delete or watch")
    conf_parser.add_argument("-l", "--log", dest="loglevel", required=False, default="info", help="Log Level for output messages, CRITICAL, ERROR, WARNING, INFO or DEBUG")
    conf_parser.add_argument("-L", "--botolog", dest="botologlevel", required=False, default="critical", help="Log Level for boto, CRITICAL, ERROR, WARNING, INFO or DEBUG")
    conf_parser.add_argument("-s", "--stack", dest="stackname", required=False, help="The stack name, used with the watch action, ignored for other actions")
    conf_parser.add_argument("-p", "--parallel", dest="parallel", required=False, default=False, action='store_true', help="Enable parallel processing (EXPERIMENTAL)")
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
        print 'Invalid log level: %s' % args.loglevel
        exit(1)
    logging.basicConfig(level=numeric_level)
    logger = logging.getLogger(__name__)

    #Get and configure the log level for boto
    if not isinstance(boto_numeric_level, int):
        logger.critical("Invalid boto log level: %s" % args.botologlevel)
        exit(1)
    logging.getLogger('boto').setLevel(boto_numeric_level)

    #Create the mega_stack object and sort out dependencies
    try:
      the_mega_stack = MegaStack(args.yamlfile, enable_parallel_mode=args.parallel)
    except CumulusException,e :
      logger.critical("CumulusException: %s"% str(e))
      return

    #Print some info about what we found in the yaml and dependency order
    logger.info("Mega stack name: %s" % the_mega_stack.name)
    logger.info("Found %s CF stacks in yaml." % len(the_mega_stack.cf_stacks))

    #Run the method of the mega stack object for the action provided
    if args.action == 'create':
        if args.parallel:
            logger.info("Parallel mode enabled. Order of execution can't be accurately predicted.")
        else:
            logger.info("Processing stacks in the following order: %s" % the_mega_stack.ordered_stacks)
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
