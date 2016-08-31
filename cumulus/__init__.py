#!/usr/bin/env python
# Peter Hall 20/03/2013
#
# Take a yaml file that describes a full VPC made of multiple CF templates

import argparse
import logging
from cumulus.MegaStack import MegaStack


def main():
    """
    Entry function for cumulus.
    """
    conf_parser = argparse.ArgumentParser()
    conf_parser.add_argument(
        "-y", "--yamlfile",
        dest="yamlfile", required=True,
        help="The yaml file to read the VPC mega stack configuration from")
    conf_parser.add_argument(
        "-a", "--action",
        dest="action", required=True,
        help="The action to preform: create, check, update, delete or watch")
    conf_parser.add_argument(
        "-l", "--log",
        dest="loglevel", required=False, default="info",
        help="Log Level for output messages,"
             " CRITICAL, ERROR, WARNING, INFO or DEBUG")
    conf_parser.add_argument(
        "-L", "--botolog",
        dest="botologlevel", required=False, default="critical",
        help="Log Level for boto, CRITICAL, ERROR, WARNING, INFO or DEBUG")
    conf_parser.add_argument(
        "-s", "--stack",
        dest="stackname", required=False,
        help="The stack name, used with the watch action,"
             " ignored for other actions")
    conf_parser.add_argument(
        "-c", "--compact",
        action="store_true", dest="compactbody", required=False,
        help="Compress each template body by removing spaces.")
    conf_parser.add_argument(
        "-p", "--prefix", metavar="PREFIX",
        dest="cf_prefix", required=False,
        help="The prefix of the created stacks. Default is the name of the mega stack.")
    conf_parser.add_argument(
        "-o", "--override-global",
        action='append', dest="override_global", required=False,
        help="Override a global variable. Example: --override-global \"globalVariable=newValue\"")
    conf_parser.add_argument(
        "--highlight",
        action="store_true", dest="highlight", required=False,
        help="Highlight output. This takes precedence over highlight-output in the yaml file.")
    conf_parser.add_argument(
        "--no-highlight",
        action="store_false", dest="highlight", required=False,
        help="Don't highlight output. This takes precedence over highlight-output in the yaml file.")
    args = conf_parser.parse_args()

    # Validate that action is something we know what to do with
    valid_actions = ['create', 'check', 'update', 'delete', 'watch']
    if args.action not in valid_actions:
        print ("Invalid action provided, must be one of: '%s'"
               % (", ".join(valid_actions)))
        exit(1)

    # Make sure we can read the yaml file provided
    try:
        open(args.yamlfile, 'r')
    except IOError as exception:
        print "Cannot read yaml file %s: %s" % (args.yamlfile, exception)
        exit(1)

    # Get and configure the log level
    numeric_level = getattr(logging, args.loglevel.upper(), None)
    boto_numeric_level = getattr(logging, args.botologlevel.upper(), None)
    if not isinstance(numeric_level, int):
        print 'Invalid log level: %s' % args.loglevel
        exit(1)
    logging.basicConfig(level=numeric_level)
    logger = logging.getLogger(__name__)

    # Get and configure the log level for boto
    if not isinstance(boto_numeric_level, int):
        logger.critical("Invalid boto log level: %s", args.botologlevel)
        exit(1)
    logging.getLogger('boto').setLevel(boto_numeric_level)

    if args.override_global:
        try:
            override_dict = dict(k for k in (x.split('=') for x in args.override_global))
        except ValueError as exception:
            print "Illegal global override parameter in: %s" % args.override_global
            exit(1)
    else:
        override_dict = None

    # Create the mega_stack object and sort out dependencies
    the_mega_stack = MegaStack(args.yamlfile, args.cf_prefix, override_dict, args.highlight)
    the_mega_stack.sort_stacks_by_deps()

    # Print some info about what we found in the yaml and dependency order
    logger.info("Mega stack name: %s", the_mega_stack.name)
    logger.info("Found %s CF stacks in yaml.", len(the_mega_stack.cf_stacks))
    logger.info("Processing stacks in the following order: %s",
                [x.name for x in the_mega_stack.stack_objs])
    for stack in the_mega_stack.stack_objs:
        logger.debug("%s depends on %s", stack.name, stack.depends_on)

    # Run the method of the mega stack object for the action provided
    if args.action == 'create':
        the_mega_stack.create(args.stackname, args.compactbody)

    if args.action == 'check':
        the_mega_stack.check(args.stackname)

    if args.action == 'delete':
        the_mega_stack.delete(args.stackname)

    if args.action == 'update':
        the_mega_stack.update(args.stackname)

    if args.action == 'watch':
        the_mega_stack.watch(args.stackname)

if __name__ == '__main__':
    main()
