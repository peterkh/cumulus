"""
Megastack module represents a logical mega stack that contains CloudFormation
stacks and the relationship between them
"""
import boto
import logging
import simplejson
import time
import yaml
import pystache
import os
from cumulus.CFStack import CFStack
from boto import cloudformation, iam


class MegaStack(object):
    """
    Main worker class for cumulus. Holds array of CFstack objects and does most
    of the calls to CloudFormation API
    """
    def __init__(self, yamlFile):
        self.logger = logging.getLogger(__name__)

        # load the yaml file and turn it into a dict
        thefile = open(yamlFile, 'r')

        rendered_file = pystache.render(thefile.read(), dict(os.environ))

        self.stackDict = yaml.safe_load(rendered_file)
        # Make sure there is only one top level element in the yaml file
        if len(self.stackDict.keys()) != 1:
            error_message = ("Need one and only one mega stack name at the" +
                             " top level, found %s")
            self.logger.critical(error_message % len(self.stackDict.keys()))
            exit(1)

        # Now we know we only have one top element,
        # that must be the mega stack name
        self.name = self.stackDict.keys()[0]

        # Find and set the mega stacks region. Exit if we can't find it
        if 'region' in self.stackDict[self.name]:
            self.region = self.stackDict[self.name]['region']
        else:
            self.logger.critical("No region specified for mega stack," +
                                 " don't know where to build it.")
            exit(1)

        if 'account_id' in self.stackDict[self.name]:
            # Get the account ID for the current AWS credentials
            iamconn = iam.connect_to_region(self.region)
            user_response = iamconn.get_user()['get_user_response']
            user_result = user_response['get_user_result']
            account_id = user_result['user']['arn'].split(':')[4]

            # Check if the current account ID matches the stack's account ID
            if account_id != str(self.stackDict[self.name]['account_id']):
                self.logger.critical("Account ID of stack does not match the" +
                                     " account ID of your AWS credentials.")
                exit(1)

        self.sns_topic_arn = self.stackDict[self.name].get('sns-topic-arn', [])
        if isinstance(self.sns_topic_arn, str):
            self.sns_topic_arn = [self.sns_topic_arn]
        for topic in self.sns_topic_arn:
            if topic.split(':')[3] != self.region:
                self.logger.critical("SNS Topic %s is not in the %s region."
                                     % (topic, self.region))
                exit(1)

        self.global_tags = self.stackDict[self.name].get('tags', {})
        # Array for holding CFStack objects once we create them
        self.stack_objs = []

        # Get the names of the sub stacks from the yaml file and sort in array
        self.cf_stacks = self.stackDict[self.name]['stacks'].keys()

        # Megastack holds the connection to CloudFormation and list of stacks
        # currently in our region stops us making lots of calls to
        # CloudFormation API for each stack
        try:
            self.cfconn = cloudformation.connect_to_region(self.region)
            self.cf_desc_stacks = self._describe_all_stacks()
        except boto.exception.NoAuthHandlerFound as exception:
            self.logger.critical(
                "No credentials found for connecting to CloudFormation: %s"
                % exception)
            exit(1)

        # iterate through the stacks in the yaml file and create CFstack
        # objects for them
        for stack_name in self.cf_stacks:
            the_stack = self.stackDict[self.name]['stacks'][stack_name]
            if type(the_stack) is dict:
                if the_stack.get('disable', False):
                    warn_message = ("Stack %s is disabled by configuration" +
                                    " directive. Skipping")
                    self.logger.warning(warn_message % stack_name)
                    continue
                local_sns_arn = the_stack.get('sns-topic-arn',
                                              self.sns_topic_arn)
                if isinstance(local_sns_arn, str):
                    local_sns_arn = [local_sns_arn]
                for topic in local_sns_arn:
                    if topic.split(':')[3] != self.region:
                        error_message = "SNS Topic %s is not in the %s region."
                        self.logger.critical(error_message
                                             % (topic, self.region))
                        exit(1)
                local_tags = the_stack.get('tags', {})
                merged_tags = dict(self.global_tags.items() +
                                   local_tags.items())
                # Add static cumulus-stack tag
                merged_tags['cumulus-stack'] = self.name
                if 'cf_template' in the_stack:
                    self.stack_objs.append(
                        CFStack(
                            mega_stack_name=self.name,
                            name=stack_name,
                            params=the_stack.get('params'),
                            template_name=the_stack['cf_template'],
                            region=self.region,
                            sns_topic_arn=local_sns_arn,
                            depends_on=the_stack.get('depends'),
                            tags=merged_tags
                        )
                    )

    def sort_stacks_by_deps(self):
        """
        Sort the array of stack_objs so they are in dependancy order
        """
        sorted_stacks = []
        dep_graph = {}
        no_deps = []
        # Add all stacks without dependancies to no_deps
        for stack in self.stack_objs:
            if stack.depends_on is None:
                no_deps.append(stack)
            else:
                dep_graph[stack.name] = stack.depends_on[:]
        # Perform a topological sort on stacks in dep_graph
        while len(no_deps) > 0:
            stack = no_deps.pop()
            sorted_stacks.append(stack)
            for node in dep_graph.keys():
                for deps in dep_graph[node]:
                    if stack.cf_stack_name == deps:
                        dep_graph[node].remove(stack.cf_stack_name)
                        if len(dep_graph[node]) < 1:
                            for stack_obj in self.stack_objs:
                                if stack_obj.name == node:
                                    no_deps.append(stack_obj)
                            del dep_graph[node]
        if len(dep_graph) > 0:
            self.logger.critical("Could not resolve dependency order." +
                                 " Either circular dependency or " +
                                 "dependency on stack not in yaml file.")
            exit(1)
        else:
            self.stack_objs = sorted_stacks
            return True

    def check(self, stack_name=None):
        """
        Checks the status of the yaml file.
        Displays parameters for the stacks it can.
        """
        for stack in self.stack_objs:
            if stack_name and stack.name != stack_name:
                continue
            self.logger.info("Starting check of stack %s" % stack.name)
            if not stack.populate_params(self.cf_desc_stacks):
                info_message = ("Could not determine correct parameters for " +
                                "CloudFormation stack %s\n\tMost likely " +
                                "because stacks it depends on haven't been " +
                                "created yet.")
                self.logger.info(info_message, stack.name)
            else:
                self.logger.info("Stack %s would be created with following "
                                 "parameter values: %s"
                                 % (stack.cf_stack_name,
                                    stack.get_params_tuples()))
                self.logger.info("Stack %s already exists in CF: %s"
                                 % (stack.cf_stack_name,
                                    bool(stack.exists_in_cf(
                                         self.cf_desc_stacks))))

    def create(self, stack_name=None):
        """
        Create all stacks in the yaml file.
        Any that already exist are skipped (no attempt to update)
        """
        for stack in self.stack_objs:
            if stack_name and stack.name != stack_name:
                continue
            self.logger.info("Starting checks for creation of stack: %s"
                             % stack.name)
            if stack.exists_in_cf(self.cf_desc_stacks):
                self.logger.info("Stack %s already exists in CloudFormation,"
                                 " skipping" % stack.name)
            else:
                if stack.deps_met(self.cf_desc_stacks) is False:
                    self.logger.critical("Dependancies for stack %s not met"
                                         " and they should be, exiting..."
                                         % stack.name)
                    exit(1)
                if not stack.populate_params(self.cf_desc_stacks):
                    self.logger.critical("Could not determine correct "
                                         "parameters for stack %s"
                                         % stack.name)
                    exit(1)

                stack.read_template()
                self.logger.info("Creating: %s, %s" % (
                    stack.cf_stack_name, stack.get_params_tuples()))
                try:
                    self.cfconn.create_stack(
                        stack_name=stack.cf_stack_name,
                        template_body=stack.template_body,
                        parameters=stack.get_params_tuples(),
                        capabilities=['CAPABILITY_IAM'],
                        notification_arns=stack.sns_topic_arn,
                        tags=stack.tags
                    )
                except Exception as exception:
                    self.logger.critical(
                        "Creating stack %s failed. Error: %s" % (
                            stack.cf_stack_name, exception))
                    exit(1)

                create_result = self.watch_events(
                    stack.cf_stack_name, "CREATE_IN_PROGRESS")
                if create_result != "CREATE_COMPLETE":
                    self.logger.critical(
                        "Stack didn't create correctly, status is now %s"
                        % create_result)
                    exit(1)

                # CF told us stack completed ok.
                # Log message to that effect and refresh the list of stack
                # objects in CF
                self.logger.info("Finished creating stack: %s"
                                 % stack.cf_stack_name)
                self.cf_desc_stacks = self._describe_all_stacks()

    def delete(self, stack_name=None):
        """
        Delete all the stacks from CloudFormation.
        Does this in reverse dependency order.
        Prompts for confirmation before deleting each stack
        """
        # Removing stacks so need to do it in reverse dependency order
        for stack in reversed(self.stack_objs):
            if stack_name and stack.name != stack_name:
                continue
            self.logger.info("Starting checks for deletion of stack: %s"
                             % stack.name)
            if not stack.exists_in_cf(self.cf_desc_stacks):
                self.logger.info(
                    "Stack %s doesn't exist in CloudFormation, skipping"
                    % stack.name)
            else:
                confirm = raw_input(
                    "Confirm you wish to delete stack %s (Name in CF: %s)"
                    " (type 'yes' if so): "
                    % (stack.name, stack.cf_stack_name))
                if not confirm == "yes":
                    self.logger.info("Not confirmed, skipping...")
                    continue
                self.logger.info("Starting delete of stack %s" % stack.name)
                self.cfconn.delete_stack(stack.cf_stack_name)
                delete_result = self.watch_events(
                    stack.cf_stack_name, "DELETE_IN_PROGRESS")
                if (delete_result != "DELETE_COMPLETE" and
                   delete_result != "STACK_GONE"):
                    self.logger.critical(
                        "Stack didn't delete correctly, status is now %s"
                        % delete_result)
                    exit(1)

                # CF told us stack completed ok. Log message to that effect and
                # refresh the list of stack objects in CF
                self.logger.info("Finished deleting stack: %s"
                                 % stack.cf_stack_name)
                self.cf_desc_stacks = self._describe_all_stacks()

    def update(self, stack_name=None):
        """
        Attempts to update each of the stacks if template or parameters are
        different to what's currently in CloudFormation.
        If a stack doesn't already exist. Logs critical error and exits.
        """
        for stack in self.stack_objs:
            if stack_name and stack.name != stack_name:
                continue
            self.logger.info("Starting checks for update of stack: %s"
                             % stack.name)
            if not stack.exists_in_cf(self.cf_desc_stacks):
                self.logger.critical(
                    "Stack %s doesn't exist in cloudformation, can't update"
                    " something that doesn't exist." % stack.name)
                exit(1)
            if not stack.deps_met(self.cf_desc_stacks):
                self.logger.critical(
                    "Dependencies for stack %s not met and they should be,"
                    " exiting..." % stack.name)
                exit(1)
            if not stack.populate_params(self.cf_desc_stacks):
                self.logger.critical("Could not determine correct parameters"
                                     " for stack %s" % stack.name)
                exit(1)
            stack.read_template()
            template_up_to_date = stack.template_uptodate(self.cf_desc_stacks)
            params_up_to_date = stack.params_uptodate(self.cf_desc_stacks)
            self.logger.debug("Stack is up to date: %s"
                              % (template_up_to_date and params_up_to_date))
            if template_up_to_date and params_up_to_date:
                self.logger.info(
                    "Stack %s is already up to date with CloudFormation,"
                    " skipping..." % stack.name)
            else:
                if not template_up_to_date:
                    self.logger.info(
                        "Template for stack %s has changed." % stack.name)
                    # Would like to get this working. Tried datadiff but can't
                    # stop it from printing whole template
                    # stack.print_template_diff(self.cf_desc_stacks)
                self.logger.info(
                    "Starting update of stack %s with parameters: %s"
                    % (stack.name, stack.get_params_tuples()))
                self.cfconn.validate_template(
                    template_body=stack.template_body)

                try:
                    self.cfconn.update_stack(
                        stack_name=stack.cf_stack_name,
                        template_body=stack.template_body,
                        parameters=stack.get_params_tuples(),
                        capabilities=['CAPABILITY_IAM'],
                        tags=stack.tags,
                    )
                except boto.exception.BotoServerError as exception:
                    try:
                        e_message_dict = simplejson.loads(exception[2])
                        if (str(e_message_dict["Error"]["Message"]) ==
                           "No updates are to be performed."):
                            self.logger.error(
                                "CloudFormation has no updates to perform on"
                                " %s, this might be because there is a "
                                "parameter with NoEcho set" % stack.name)
                            continue
                        else:
                            self.logger.error(
                                "Got error message: %s"
                                % e_message_dict["Error"]["Message"])
                            raise exception
                    except simplejson.decoder.JSONDecodeError:
                        self.logger.critical(
                            "Unknown error updating stack: %s", exception)
                        exit(1)
                update_result = self.watch_events(
                    stack.cf_stack_name, [
                        "UPDATE_IN_PROGRESS",
                        "UPDATE_COMPLETE_CLEANUP_IN_PROGRESS"])
                if update_result != "UPDATE_COMPLETE":
                    self.logger.critical(
                        "Stack didn't update correctly, status is now %s"
                        % update_result)
                    exit(1)

                self.logger.info(
                    "Finished updating stack: %s" % stack.cf_stack_name)

            # avoid getting rate limited
            time.sleep(2)

    def watch(self, stack_name):
        """
        Watch events for a given CloudFormation stack.
        It will keep watching until its state changes
        """
        if not stack_name:
            self.logger.critical(
                "No stack name passed in, nothing to watch... use -s to "
                "provide stack name.")
            exit(1)
        the_stack = False
        for stack in self.stack_objs:
            if stack_name == stack.name:
                the_stack = stack
        if not the_stack:
            self.logger.error("Cannot find stack %s to watch" % stack_name)
            return False
        the_cf_stack = the_stack.exists_in_cf(self.cf_desc_stacks)
        if not the_cf_stack:
            self.logger.error(
                "Stack %s doesn't exist in CloudFormation, can't watch "
                "something that doesn't exist." % stack_name)
            return False

        self.logger.info(
            "Watching stack %s, while in state %s."
            % (the_stack.cf_stack_name, str(the_cf_stack.stack_status)))
        self.watch_events(
            the_stack.cf_stack_name, str(the_cf_stack.stack_status))

    def watch_events(self, stack_name, while_status):
        """
        Used by the various actions to watch CloudFormation events
        while a stacks in a given state
        """
        try:
            cfstack_obj = self.cfconn.describe_stacks(stack_name)[0]
            events = list(self.cfconn.describe_stack_events(stack_name))
        except boto.exception.BotoServerError as exception:
            if (str(exception.error_message) ==
               "Stack:%s does not exist" % (stack_name)):
                return "STACK_GONE"

        colors = {
            'blue': '\033[0;34m',
            'red': '\033[0;31m',
            'bred': '\033[1;31m',
            'green': '\033[0;32m',
            'bgreen': '\033[1;32m',
            'yellow': '\033[0;33m',
        }

        status_color_map = {
            'CREATE_IN_PROGRESS': colors['blue'],
            'CREATE_FAILED': colors['bred'],
            'CREATE_COMPLETE': colors['green'],
            'ROLLBACK_IN_PROGRESS': colors['red'],
            'ROLLBACK_FAILED': colors['bred'],
            'ROLLBACK_COMPLETE': colors['yellow'],
            'DELETE_IN_PROGRESS': colors['red'],
            'DELETE_FAILED': colors['bred'],
            'DELETE_COMPLETE': colors['yellow'],
            'UPDATE_IN_PROGRESS': colors['blue'],
            'UPDATE_COMPLETE_CLEANUP_IN_PROGRESS': colors['blue'],
            'UPDATE_COMPLETE': colors['bgreen'],
            'UPDATE_ROLLBACK_IN_PROGRESS': colors['red'],
            'UPDATE_ROLLBACK_FAILED': colors['bred'],
            'UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS': colors['red'],
            'UPDATE_ROLLBACK_COMPLETE': colors['yellow'],
            'UPDATE_FAILED': colors['bred'],
        }
        # print the last 5 events, so we get to see the start of the action we
        # are performing
        self.logger.info("Last 5 events for this stack:")
        for event in reversed(events[:5]):
            if self.stackDict[self.name].get('highlight-output', True):
                self.logger.info("%s %s%s\033[0m %s %s %s %s" % (
                    event.timestamp.isoformat(),
                    status_color_map.get(event.resource_status, ''),
                    event.resource_status,
                    event.resource_type,
                    event.logical_resource_id,
                    event.physical_resource_id,
                    event.resource_status_reason,
                ))
            else:
                self.logger.info("%s %s %s %s %s %s" % (
                    event.timestamp.isoformat(),
                    event.resource_status,
                    event.resource_type,
                    event.logical_resource_id,
                    event.physical_resource_id,
                    event.resource_status_reason,
                ))
        status = str(cfstack_obj.stack_status)
        self.logger.info("New events:")
        while status in while_status:
            try:
                new_events = list(
                    self.cfconn.describe_stack_events(stack_name))
            except boto.exception.BotoServerError as exception:
                if (str(exception.error_message) ==
                   "Stack:%s does not exist" % (stack_name)):
                    return "STACK_GONE"
            count = 0
            events_to_log = []
            while (events[0].timestamp != new_events[count].timestamp or
                   events[0].logical_resource_id !=
                   new_events[count].logical_resource_id):
                events_to_log.insert(0, new_events[count])
                count += 1
            for event in events_to_log:
                if self.stackDict[self.name].get('highlight-output', True):
                    self.logger.info("%s %s%s\033[0m %s %s %s %s" % (
                        event.timestamp.isoformat(),
                        status_color_map.get(event.resource_status, ''),
                        event.resource_status,
                        event.resource_type,
                        event.logical_resource_id,
                        event.physical_resource_id,
                        event.resource_status_reason,
                    ))
                else:
                    self.logger.info("%s %s %s %s %s %s" % (
                        event.timestamp.isoformat(),
                        event.resource_status,
                        event.resource_type,
                        event.logical_resource_id,
                        event.physical_resource_id,
                        event.resource_status_reason,
                    ))
            if count > 0:
                events = new_events[:]
            cfstack_obj.update()
            status = str(cfstack_obj.stack_status)
            time.sleep(5)
        return status

    def _describe_all_stacks(self):
        """
        Get all pages of stacks from describe_stacks API call.
        """
        result = []
        resp = self.cfconn.describe_stacks()
        result.extend(resp)
        while resp.next_token:
            resp = self.cfconn.describe_stacks(next_token=resp.next_token)
            result.extend(resp)
        return result
