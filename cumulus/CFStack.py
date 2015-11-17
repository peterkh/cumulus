"""
CFStack module. Manages a single CloudFormation stack.
"""
import logging
import simplejson


class CFStack(object):
    """
    CFstack object represents a CloudFormation stack including its parameters,
    region, template and what other stacks it depends on.
    """
    def __init__(self, mega_stack_name, name, params, template_name, region,
                 sns_topic_arn, cf_object, tags=None, depends_on=None):
        self.logger = logging.getLogger(__name__)
        # the object that will interface to CloudFormation API for us
        self.cf_object = cf_object
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

        # Safer than setting default value for tags = {}
        if tags is None:
            self.tags = {}
        else:
            self.tags = tags

        # because boto3, we need tags in a list as well
        self.tags_list = []
        for tag in self.tags:
            self.tags_list.append({'Key': tag, 'Value': self.tags[tag]})

        try:
            open(template_name, 'r')
        except:
            self.logger.critical("Failed to open template file %s for stack %s"
                                 % (self.template_name, self.name))
            exit(1)

        # check params is a dict if set
        if self.yaml_params and type(self.yaml_params) is not dict:
            self.logger.critical(
                "Parameters for stack %s must be of type dict not %s",
                self.name, type(self.yaml_params))
            exit(1)

        self.cf_stacks = {}
        self.cf_stacks_resources = {}

    def deps_met(self):
        """
        Check whether stacks we depend on exist in CloudFormation
        """
        if self.depends_on is None:
            return True
        else:
            for dep in self.depends_on:
                dep_met = False
                # check CF if stack we depend on has been created successfully
                existing_stacks = self.cf_object.list_existing_stacks()
                for stack in existing_stacks:
                    if stack == dep:
                        dep_met = True
                if not dep_met:
                    return False
            return True

    def exists(self):
        """Check if this stack exists in CloudFormation."""
        return self.cf_object.exists(self.cf_stack_name)

    def cf_details(self):
        """Return the stack name from CloudFormation and current state."""
        return self.cf_object.get_stack_status(self.cf_stack_name)

    def populate_params(self):
        """Populate the parameter list for this stack."""
        # If we have no parameters in the yaml file,
        # set params to an empty dict and return true
        if self.yaml_params is None:
            self.params = {}
            return True
        if self.deps_met():
            for param_name, param_val in self.yaml_params.iteritems():
                if type(param_val) is dict:
                    self.params[param_name] = self._parse_param(
                        param_name, param_val)
                # If param_val is a list it means there is an array of vars
                # we need to turn into a comma sep list.
                elif type(param_val) is list:
                    param_list = []
                    for item in param_val:
                        if type(item) is dict:
                            param_list.append(self._parse_param(
                                param_name, str(item['value'])))
                    self.params[param_name] = ','.join(param_list)
            return True
        else:
            return False

    def _parse_param(self, param_name, param_dict):
        """
        Parse a param dict and return var value or false if not valid
        """
        # Static value set, so use it
        if 'value' in param_dict:
            return str(param_dict['value'])
        # No static value set, but if we have a source,
        # type and variable can try getting from CF
        elif ('source' in param_dict
              and 'type' in param_dict
              and 'variable' in param_dict):
            if param_dict['source'] == self.mega_stack_name:
                source_stack = param_dict['source']
            else:
                source_stack = ("%s-%s" %
                                (self.mega_stack_name, param_dict['source']))
            return self.get_value_from_cf(
                source_stack=source_stack,
                var_type=param_dict['type'],
                var_name=param_dict['variable'])
        else:
            error_message = ("Error in yaml file, can't parse parameter %s"
                             " for %s stack.")
            self.logger.critical(error_message, param_name, self.name)
            exit(1)

    def get_value_from_cf(self, source_stack, var_type, var_name):
        """
        Get a variable from a existing cloudformation stack, var_type should be
        parameter, resource or output.
        If using resource, provide the logical ID and this will return the
        Physical ID
        """
        if var_type == 'resource':
            the_stack = self.cf_object.describe_stack(
                source_stack, resources=True)
            for res in the_stack['resources']:
                if str(res['LogicalResourceId']) == var_name:
                    return str(res['PhysicalResourceId'])
        else:
            the_stack = self.cf_object.describe_stack(source_stack)
            if var_type == 'parameter':
                for param in the_stack['Parameters']:
                    if str(param['ParameterKey']) == var_name:
                        return str(param['ParameterValue'])
            elif var_type == 'output':
                for output in the_stack['Outputs']:
                    if str(output['OutputKey']) == var_name:
                        return str(output['OutputValue'])
            else:
                error_message = ("Error: invalid var_type passed to"
                                 " get_value_from_cf, needs to be parameter, "
                                 "resource or output. Not: %s")
                self.logger.critical(error_message, (var_type))
                exit(1)

    def get_params_tuples(self):
        """Convert param dict to array of tuples needed by boto."""
        tuple_list = []
        if len(self.params) > 0:
            for param in self.params.keys():
                tuple_list.append((param, self.params[param]))
        return tuple_list

    def get_params_boto3(self):
        """Convert param dict to array of tuples needed by boto3."""
        param_list = []
        for param in self.params.keys():
            param_list.append({
                'ParameterKey': param,
                'ParameterValue': self.params[param]})

        return param_list

    def read_template(self):
        """Open and parse the json template for this stack."""
        try:
            template_file = open(self.template_name, 'r')
            template = simplejson.load(template_file)
        except Exception as exception:
            self.logger.critical("Cannot parse %s template for stack %s."
                                 " Error: %s", self.template_name, self.name,
                                 exception)
            exit(1)
        self.template_body = simplejson.dumps(
            template,
            sort_keys=True,
            indent=2,
            separators=(',', ': '),
        )
        return True

    def template_uptodate(self):
        """
        Check if stack is up to date with cloudformation.

        Returns true if template matches what's in cloudformation,
        false if not or stack not found.
        """
        cf_temp_dict = self.cf_object.get_template(self.cf_stack_name)
        if cf_temp_dict == simplejson.loads(self.template_body):
            return True
        return False

    def params_uptodate(self):
        """Check if parameters in stack are up to date with Cloudformation."""
        stack_details = self.cf_object.describe_stack(self.cf_stack_name)
        parameters = stack_details.get('Parameters', [])

        # If number of params in CF and this stack obj dont match,
        # then it needs updating
        if len(parameters) != len(self.params):
            self.logger.debug("New and old parameter lists are different "
                              "lengths for %s", self.name)
            return False

        for param in parameters:
            # check if param in CF exists in our new parameter set,
            # if not they are differenet and need updating
            key = param['ParameterKey']
            value = param['ParameterValue']
            if key not in self.params:
                msg = ("New params are missing key %s that exists in CF for "
                       "%s stack already.")
                self.logger.debug(msg, key, self.name)
                return False
            # if the value of parameters are different, needs updating
            if self.params[key] != value:
                msg = "Param %s for stack %s has changed from %s to %s"
                self.logger.debug(msg, key, self.name,
                                  value, self.params[key])
                return False

        # We got to the end without returning False, so must be fine.
        return True

    def create(self):
        """Create this stack in CloudFormation."""
        self.cf_object.create_stack(
            stack_name=self.cf_stack_name,
            template_body=self.template_body,
            parameters=self.get_params_boto3(),
            notification_arns=self.sns_topic_arn,
            tags=self.tags_list
            )

    def delete(self):
        """Delete this stack in CloudFormation."""
        self.cf_object.delete_stack(stack_name=self.cf_stack_name)

    def update(self):
        """Update this stack in CloudFormation."""
        self.cf_object.update_stack(stack_name=self.cf_stack_name)
