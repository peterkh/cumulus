import boto
import logging
import simplejson
import time
import yaml
from CFStack import CFStack
from boto import cloudformation

from .exceptions import DependencyLoopError, StackStatusInconsistent
from .graph import StackDependencyGraph



class MegaStack:
    """
    Main workder class for cumulus. Holds array of CFstack objects and does most of the calls to cloudformation API
    """
    def __init__(self, yamlFile, enable_parallel_mode):
        self.parallel = enable_parallel_mode
        self.logger = logging.getLogger(__name__)
        
        #load the yaml file and turn it into a dict
        thefile = open(yamlFile, 'r')
        self.stackDict = yaml.safe_load(thefile)

        #Make sure there is only one top level element in the yaml file
        if len(self.stackDict.keys()) != 1:
            self.logger.critical("Need one and only one mega stack name at the top level, found %s" % len(self.stackDict.keys()))
            exit(1)
        
        #How we know we only have one top element, that must be the mega stack name
        self.name = self.stackDict.keys()[0]

        #Find and set the mega stacks region. Exit if we can't find it
        if self.stackDict[self.name].has_key('region'):
            self.region = self.stackDict[self.name]['region']
        else:
            self.logger.critical("No region specified for mega stack, don't know where to build it.")
            exit(1)

        self.sns_topic_arn = self.stackDict[self.name].get('sns-topic-arn', [])
        if isinstance(self.sns_topic_arn, str): self.sns_topic_arn = [self.sns_topic_arn]
        for topic in self.sns_topic_arn:
            if topic.split(':')[3] != self.region:
                self.logger.critical("SNS Topic %s is not in the %s region." % (topic, self.region))
                exit(1)

        #Hash for holding CFStack objects once we create them
        self.stack_objs = dict()

        #Get the names of the sub stacks from the yaml file and sort in array
        self.cf_stacks = self.stackDict[self.name]['stacks'].keys()
        
        #Megastack holds the connection to cloudformation and list of stacks currently in our region
        #Stops us making lots of calls to cloudformation API for each stack
        try:
            self.cfconn = cloudformation.connect_to_region(self.region)
            self.cf_desc_stacks = self.cfconn.describe_stacks()
        except boto.exception.NoAuthHandlerFound as e:
            self.logger.critical("No credentials found for connecting to cloudformation: %s" % e )
            exit(1)

        #iterate through the stacks in the yaml file and create CFstack objects for them
        for stack_name in self.cf_stacks:
            the_stack = self.stackDict[self.name]['stacks'][stack_name]
            if type(the_stack) is dict:
                local_sns_arn = the_stack.get('sns-topic-arn', self.sns_topic_arn)
                if isinstance(local_sns_arn, str): local_sns_arn = [local_sns_arn]
                for topic in local_sns_arn:
                    if topic.split(':')[3] != self.region:
                        self.logger.critical("SNS Topic %s is not in the %s region." % (topic, self.region))
                        exit(1)
                if the_stack.has_key('cf_template'):
                    self.stack_objs[stack_name] = CFStack(
                            mega_stack_name=self.name,
                            name=stack_name,
                            params=the_stack['params'],
                            template_name=the_stack['cf_template'],
                            region=self.region,
                            sns_topic_arn=local_sns_arn,
                            depends_on=the_stack['depends']
                    )
        self.build_dep_graph()
        self.ordered_stacks = list(self.dep_graph.linear_traversal(self.name))


    def build_dep_graph(self):
      self.dep_graph = StackDependencyGraph()
      for stack_id in self.stack_objs:
        self.logger.debug("Adding node %s to graph" % stack_id)
        self.dep_graph.add_node(stack_id)
      for stack_id in self.stack_objs:
        stack = self.stack_objs[stack_id]
        if stack.depends_on is not None:
          for stack_dep in stack.depends_on:
            self.logger.debug("Adding relation to graph: %s depends on %s" % (stack.name, stack_dep))
            self.dep_graph.add_dependency(stack_dep, stack.name)
      loops = self.dep_graph.find_cycle()
      if len(loops):
        raise DependencyLoopError(loops)


    def check(self):
        """
        Checks the status of the yaml file. Displays parameters for the stacks it can.
        """
        for stack_id in self.ordered_stacks:
            stack = self.stack_objs[stack_id]
            self.logger.info("Starting check of stack %s" % stack.name)
            if not stack.populate_params(self.cf_desc_stacks):
                self.logger.info("Could not determine correct parameters for Cloudformation stack %s\n" % stack.name + 
                        "\tMost likely because stacks it depends on haven't been created yet.")
            else:
                self.logger.info("Stack %s would be created with following parameter values: %s" % (stack.cf_stack_name, stack.get_params_tuples()))
                self.logger.info("Stack %s already exists in CF: %s" % (stack.cf_stack_name, bool(stack.exists_in_cf(self.cf_desc_stacks))))

    def adjust_graph(self):
        for node in self.ordered_stacks:
            if node not in self.dep_graph.nodes():
                continue
            self.logger.debug("Checking status of %s" % node)
            stack = self.stack_objs[node]
            stack_status = stack.get_status(self.cf_desc_stacks)
            if not stack_status:
                # Stack doesn't exist
                continue
            if stack_status in ("CREATE_COMPLETE", "UPDATE_COMPLETE"):
                self.logger.info("Stack %s is complete" % stack.name)
                self.dep_graph.del_node(node)
            elif stack_status.endswith("_PROGRESS"):
                self.logger.debug("Stack %s is still being processed" % stack.name)
            else:
                self.logger.critical("Stack %s is in inconsistent state: %s, manual intervention is required" % (stack.name, stack_status))
                raise StackStatusInconsistent(stack.name, stack_status)


    def create_stack(self, stack):
        if not stack.exists_in_cf(self.cf_desc_stacks):
            if stack.populate_params(self.cf_desc_stacks):
                stack.read_template()
                self.logger.info("Creating: %s" % (stack.cf_stack_name))
                try:
                    pass
                    self.cfconn.create_stack(
                        stack_name=stack.cf_stack_name,
                        template_body=stack.template_body,
                        parameters=stack.get_params_tuples(),
                        capabilities=['CAPABILITY_IAM'],
                        notification_arns=stack.sns_topic_arn
                    )
                except Exception as e:
                    self.logger.critical("Creating stack %s failed. Error: %s" % (stack.cf_stack_name, e))
                    exit(1)
            else:
                self.logger.critical("Could not determine correct parameters for stack %s" % stack.name)

    def create(self):
        if self.parallel:
            self.adjust_graph()
            edge_nodes = self.dep_graph.get_edge_nodes()
            while edge_nodes:
                self.logger.debug("The followings stacks are free to go: %s" % str(edge_nodes))
                for node in edge_nodes:
                    self.logger.debug("Processing %s" % node)
                    stack = self.stack_objs[node]
                    try:
                        self.create_stack(stack)
                    except Exception as e:
                        self.logger.critical("Creating stack %s failed. Error: %s" % (stack.cf_stack_name, e))
                        exit(1)
                self.logger.debug("Sleeping a few secs")
                time.sleep(3)
                self.cf_desc_stacks = self.cfconn.describe_stacks()
                self.adjust_graph()
                edge_nodes = self.dep_graph.get_edge_nodes()
        else:
            for node in self.ordered_stacks:
                stack = self.stack_objs[node]
                self.logger.info("Starting checks for creation of stack: %s" % stack.name)
                if stack.exists_in_cf(self.cf_desc_stacks):
                    self.logger.info("Stack %s already exists in cloudformation, skipping" % stack.name)
                else:
                    if stack.deps_met(self.cf_desc_stacks) is False:
                        self.logger.critical("Dependancies for stack %s not met and they should be, exiting..." % stack.name)
                        exit(1)
                try:
                    self.create_stack(stack)
                except Exception as e:
                    self.logger.critical("Creating stack %s failed. Error: %s" % (stack.cf_stack_name, e))
                    exit(1)
                create_result = self.watch_events(stack.cf_stack_name, "CREATE_IN_PROGRESS")
                if create_result != "CREATE_COMPLETE":
                    self.logger.critical("Stack didn't create correctly, status is now %s" % create_result)
                    exit(1)

                #CF told us stack completed ok. Log message to that effect and refresh the list of stack objects in CF
                self.logger.info("Finished creating stack: %s" % stack.cf_stack_name)
                self.cf_desc_stacks = self.cfconn.describe_stacks()


    def delete(self):
        """
        Delete all the stacks from cloudformation.
        Does this in reverse dependency order. Prompts for confirmation before deleting each stack
        """
        #Removing stacks so need to do it in reverse dependancy order
        for node in reversed(self.ordered_stacks):
            stack = self.stack_objs[node]
            self.logger.info("Starting checks for deletion of stack: %s" % stack.name)
            if not stack.exists_in_cf(self.cf_desc_stacks):
                self.logger.info("Stack %s doesn't exist in cloudformation, skipping" % stack.name)
            else:
                confirm = raw_input("Confirm you wish to delete stack %s (Name in CF: %s) (type 'yes' if so): " % (stack.name, stack.cf_stack_name))
                if not confirm == "yes":
                    print "Not confirmed, skipping..."
                    continue
                self.logger.info("Starting delete of stack %s" % stack.name)
                self.cfconn.delete_stack(stack.cf_stack_name)
                delete_result = self.watch_events(stack.cf_stack_name, "DELETE_IN_PROGRESS")
                if delete_result != "DELETE_COMPLETE" and delete_result != "STACK_GONE":
                    self.logger.critical("Stack didn't delete correctly, status is now %s" % delete_result)
                    exit(1)

                #CF told us stack completed ok. Log message to that effect and refresh the list of stack objects in CF
                self.logger.info("Finished deleting stack: %s" % stack.cf_stack_name)
                self.cf_desc_stacks = self.cfconn.describe_stacks()

    def update(self):
        """
        Attempts to update each of the stacks if template or parameters are diffenet to whats currently in cloudformation
        If a stack doesn't already exist. Logs critical error and exits.
        """
        for node in self.ordered_stacks:
            stack = self.stack_objs[node]
            self.logger.info("Starting checks for update of stack: %s" % stack.name)
            if not stack.exists_in_cf(self.cf_desc_stacks):
                self.logger.critical("Stack %s doesn't exist in cloudformation, can't update something that doesn't exist." % stack.name)
                exit(1)
            if not stack.deps_met(self.cf_desc_stacks):
                self.logger.critical("Dependancies for stack %s not met and they should be, exiting..." % stack.name)
                exit(1)
            if not stack.populate_params(self.cf_desc_stacks):
                self.logger.critical("Could not determine correct parameters for stack %s" % stack.name)
                exit(1)
            stack.read_template()
            template_up_to_date = stack.template_uptodate(self.cf_desc_stacks)
            params_up_to_date = stack.params_uptodate(self.cf_desc_stacks)
            self.logger.debug("Stack is up to date: %s" % (template_up_to_date and params_up_to_date))
            if template_up_to_date and params_up_to_date:
                self.logger.info("Stack %s is already up to date with cloudformation, skipping..." % stack.name)
            else:
                if not template_up_to_date:
                    self.logger.info("Template for stack %s has changed." % stack.name)
                    #Would like to get this working. Tried datadiff at the moment but can't stop it from printing whole template
                    #stack.print_template_diff(self.cf_desc_stacks)
                self.logger.info("Starting update of stack %s with parameters: %s" % (stack.name, stack.get_params_tuples()))
                self.cfconn.validate_template(template_body = stack.template_body)
                try:
                    self.cfconn.update_stack(
                            stack_name    = stack.cf_stack_name,
                            template_body = stack.template_body,
                            parameters    = stack.get_params_tuples(),
                            capabilities  = ['CAPABILITY_IAM']
                            )
                except boto.exception.BotoServerError as e:
                    e_message_dict = simplejson.loads(e.error_message)
                    if str(e_message_dict["Error"]["Message"]) == "No updates are to be performed.":
                        self.logger.error("Cloudformation has no updates to perform on %s, this might be because there is a parameter with NoEcho set" % stack.name)
                        continue
                    else:
                        self.logger.debug("Got error message: %s" % e_message_dict["Error"]["Message"])
                        raise e
                update_result = self.watch_events(stack.cf_stack_name, ["UPDATE_IN_PROGRESS", "UPDATE_COMPLETE_CLEANUP_IN_PROGRESS"])
                if update_result != "UPDATE_COMPLETE":
                    self.logger.critical("Stack didn't update correctly, status is now %s" % update_result)
                    exit(1)
                
                self.logger.info("Finished updating stack: %s" % stack.cf_stack_name)

    
    def watch(self, stack_name):
        """
        Watch events for a given cloudformation stack. It will keep watching until its state changes
        """
        if not stack_name:
            self.logger.critical("No stack name passed in, nothing to watch... use -s to provide stack name.")
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
            self.logger.error("Stack %s doesn't exist in cloudformation, can't watch something that doesn't exist." % stack.name)
            return False
        
        self.logger.info("Watching stack %s, while in state %s." % (the_stack.cf_stack_name, str(the_cf_stack.stack_status)))
        self.watch_events(the_stack.cf_stack_name, str(the_cf_stack.stack_status))


    def watch_events(self, stack_name, while_status):
        """
        Used by the various actions to watch cloudformation events while a stacks in a given state
        """
        try:
            cfstack_obj = self.cfconn.describe_stacks(stack_name)[0]
            events = list(self.cfconn.describe_stack_events(stack_name))
        except boto.exception.BotoServerError as e:
            if str(e.error_message) == "Stack:%s does not exist" % (stack_name):
                return "STACK_GONE"
        #print the last 5 events, so we get to see the start of the action we are performing
        self.logger.info("Last 5 events for this stack:")
        for e in reversed(events[:5]):
            self.logger.info("%s %s %s %s %s %s" % (e.timestamp.isoformat(), e.resource_status, e.resource_type, e.logical_resource_id, e.physical_resource_id, e.resource_status_reason))
        status = str(cfstack_obj.stack_status)
        self.logger.info("New events:")
        while status in while_status:
            try:
                new_events = list(self.cfconn.describe_stack_events(stack_name))
            except boto.exception.BotoServerError as e:
                if str(e.error_message) == "Stack:%s does not exist" % (stack_name):
                    return "STACK_GONE"
            x = 0
            events_to_log = []
            while events[0].timestamp != new_events[x].timestamp or events[0].logical_resource_id != new_events[x].logical_resource_id:
                events_to_log.insert(0, new_events[x])
                x += 1
            for event in events_to_log:
                self.logger.info("%s %s %s %s %s %s" % (event.timestamp.isoformat(), event.resource_status, event.resource_type, event.logical_resource_id, event.physical_resource_id, event.resource_status_reason))
            if x > 0:
                events = new_events[:]
            cfstack_obj.update()
            status = str(cfstack_obj.stack_status)
            time.sleep(5)
        return status


