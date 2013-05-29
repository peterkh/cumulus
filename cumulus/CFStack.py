import logging
import simplejson
from boto import cloudformation


class CFStack:
    def __init__(self, mega_stack_name, name, params, template_name, region, sns_topic_arn, depends_on = None):
        self.logger = logging.getLogger(__name__)
        if mega_stack_name == name:
            self.cf_stack_name = name
        else:
            self.cf_stack_name = "%s-%s" % (mega_stack_name, name)
        self.mega_stack_name = mega_stack_name 
        self.name = name
        self.yaml_params = params
        self.params = {}
        self.template_name = template_name
        self.template_body = ''
        if depends_on is None:
            self.depends_on = None
        else:
            self.depends_on = []
            for dep in depends_on:
                if dep == mega_stack_name:
                    self.depends_on.append(dep)
                else:
                    self.depends_on.append("%s-%s" % (mega_stack_name, dep))
        self.region = region
        self.sns_topic_arn = sns_topic_arn

        try:
            open(template_name, 'r')
        except:
            self.logger.critical("Failed to open template file %s for stack %s" % (self.template_name, self.name))
            exit(1)
        if not ( type(self.yaml_params) is dict or self.yaml_params is None ):
            self.logger.critical("Parameters for stack %s must be of type dict not %s" % (self.name, type(self.yaml_params)))
            exit(1)

        self.cf_stacks = {}
        self.cf_stacks_resources = {}
    
    def deps_met(self, current_cf_stacks):
        if self.depends_on is None:
            return True
        else:
            for dep in self.depends_on:
                dep_met = False
                #check CF if stack we depend on has been created successfully
                for stack in current_cf_stacks:
                    if str(stack.stack_name) == dep:
                        dep_met = True
                if not dep_met:
                    return False
            return True
    
    def exists_in_cf(self, current_cf_stacks):
        for stack in current_cf_stacks:
            if str(stack.stack_name) == self.cf_stack_name:
                return stack
        return False

    def populate_params(self, current_cf_stacks):
        #If we have no parameters in the yaml file, set params to an empty dict and return true
        if self.yaml_params is None:
            self.params = {}
            return True
        if self.deps_met(current_cf_stacks):
            cfconn = cloudformation.connect_to_region(self.region)
            for param in self.yaml_params.keys():
                if type(self.yaml_params[param]) is dict:
                    #Static value set, so use it
                    if self.yaml_params[param].has_key('value'):
                        self.params[param] = str(self.yaml_params[param]['value'])
                    #No static value set, but if we have a source, type and variable can try getting from CF
                    elif self.yaml_params[param].has_key('source') and self.yaml_params[param].has_key('type') and self.yaml_params[param].has_key('variable'):
                        if self.yaml_params[param]['source'] == self.mega_stack_name:
                            source_stack = self.yaml_params[param]['source']
                        else:
                            source_stack = "%s-%s" % (self.mega_stack_name, self.yaml_params[param]['source'])
                        self.params[param] = self.get_value_from_cf(
                                source_stack = source_stack,
                                var_type = self.yaml_params[param]['type'],
                                var_name = self.yaml_params[param]['variable']
                                )
                #If self.yaml_params[param] is a list it means there is an array of vars we need to turn into a comma sep list.
                elif type(self.yaml_params[param]) is list:
                    param_list = []
                    for item in self.yaml_params[param]:
                        if type(item) is dict:
                            #Static value set, so use it
                            if item.has_key('value'):
                                param_list.append(str(item['value']))
                            #No static value set, but if we have a source, type and variable can try getting from CF
                            elif item.has_key('source') and item.has_key('type') and item.has_key('variable'):
                                if item['source'] == self.mega_stack_name:
                                    source_stack = item['source']
                                else:
                                    source_stack = "%s-%s" % (self.mega_stack_name, item['source'])
                                param_list.append(self.get_value_from_cf(
                                    source_stack = source_stack,
                                    var_type = item['type'],
                                    var_name = item['variable']
                                    ))
                            else:
                                print "Error in yaml file, %s in parameter list for %s stack. Can't populate." % (self.yaml_params[param],self.name)
                                exit(1)
                    self.params[param] = ','.join(param_list)
            return True
        else:
            return False

    def get_cf_stack(self, stack, resources = False):
        """
        Get information on parameters, outputs and resources from a stack and cache it
        """
        if not resources: 
            if not self.cf_stacks.has_key(stack):
                #We don't have this stack in the cache already so we need to pull it from CF
                cfconn = cloudformation.connect_to_region(self.region)
                self.cf_stacks[stack] = cfconn.describe_stacks(stack)[0]
            return self.cf_stacks[stack]
        else:
            if not self.cf_stacks_resources.has_key(stack):
                cfconn = cloudformation.connect_to_region(self.region)
                the_stack = self.get_cf_stack(stack = stack, resources = False)
                self.cf_stacks_resources[stack] = the_stack.list_resources()
            return self.cf_stacks_resources[stack]

    def get_value_from_cf(self, source_stack, var_type, var_name):
        """
        Get a variable from a existing cloudformation stack, var_type should be parameter, resource or output.
        If using resource, provide the logical ID and this will return the Physical ID
        """
        cfconn = cloudformation.connect_to_region(self.region)

        the_stack = self.get_cf_stack(stack = source_stack)
        if var_type == 'parameter':
            for p in the_stack.parameters:
                if str(p.key) == var_name:
                    return str(p.value)
        elif var_type == 'output':
            for o in the_stack.outputs:
                if str(o.key) == var_name:
                    return str(o.key)
        elif var_type == 'resource':
            for r in self.get_cf_stack(stack = source_stack, resources = True):
                if str(r.logical_resource_id) == var_name:
                    return str(r.physical_resource_id)
        else:
            print "Error: invalid var_type passed to get_value_from_cf, needs to be parameter, resource or output. Not: %s" % (var_type)
            exit(1)

            

    def get_params_tuples(self):
        tuple_list = []
        if len(self.params) > 0:
            for param in self.params.keys():
                tuple_list.append((param, self.params[param]))
        return tuple_list

    def read_template(self):
        try:
            template_file = open(self.template_name, 'r')
            template = simplejson.load(template_file)
        except Exception as e:
            print "Cannot open template file for stack %s, error: %s" % (self.name, e)
            exit(1)
        self.template_body = simplejson.dumps(template)
        return True

    
    def template_uptodate(self, current_cf_stacks):
        """
        Check if stack is up to date with cloudformation.
        Returns true if template matches whats in cloudformation, false if not or stack not found.
        """
        cf_stack = self.exists_in_cf(current_cf_stacks)
        if not cf_stack:
            return False
        cf_template_dict = simplejson.loads(cf_stack.get_template()['GetTemplateResponse']['GetTemplateResult']['TemplateBody'])
        if cf_template_dict == simplejson.loads(self.template_body):
            return True
        else:
            return False

    def params_uptodate(self, current_cf_stacks):
        """
        Check if parameters in stack are up to date with Cloudformation
        """
        cf_stack = self.exists_in_cf(current_cf_stacks)
        if not cf_stack:
            return False

        #If number of params in CF and this stack obj dont match, then it needs updating
        if len(cf_stack.parameters) != len(self.params):
            self.logger.debug("New and old parameter lists are different lengths for %s" % (self.name))
            return False

        for param in cf_stack.parameters:
            #check if param in CF exists in our new parameter set, 
            #if not they are differenet and need updating
            if not self.params.has_key(str(param.key)):
                self.logger.debug("New params are missing key %s that exists in CF for %s stack already." % (str(param.key), self.name))
                return False
            #if the value of parameters are different, needs updating
            if self.params[str(param.key)] != str(param.value):
                self.logger.debug("Param %s for stack %s has changed from %s to %s" % (str(param.key), self.name, str(param.value), self.params[str(param.key)]))
                return False
        
        #We got to the end without returning False, so must be fine.
        return True

    def print_template_diff(self, current_cf_stacks):
        cf_stack = self.exists_in_cf(current_cf_stacks)
        cf_template_dict = simplejson.loads(cf_stack.get_template()['GetTemplateResponse']['GetTemplateResult']['TemplateBody'])

        self.logger.info(datadiff.diff(cf_template_dict, simplejson.loads(self.template_body), context=0))
        
