"""
CFStack module. Manages a single CloudFormation stack.
"""
import logging
import simplejson
import yaml
import os
import boto
import datetime
import operator
import pytz
from boto import cloudformation
from boto.exception import S3ResponseError


class CFStack(object):
    """
    CFstack object represents a CloudFormation stack including its parameters,
    region, template and what other stacks it depends on.
    """
    def __init__(self, mega_stack_name, name, params, template_name, region,
                 sns_topic_arn, tags=None, depends_on=None):
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

        # Safer than setting default value for tags = {}
        if tags is None:
            self.tags = {}
        else:
            self.tags = tags

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

    def deps_met(self, current_cf_stacks):
        """
        Check whether stacks we depend on exist in CloudFormation
        """
        if self.depends_on is None:
            return True
        else:
            for dep in self.depends_on:
                dep_met = False
                # check CF if stack we depend on has been created successfully
                for stack in current_cf_stacks:
                    if str(stack.stack_name) == dep:
                        dep_met = True
                if not dep_met:
                    return False
            return True

    def exists_in_cf(self, current_cf_stacks):
        """
        Check if this stack exists in CloudFormation
        """
        for stack in current_cf_stacks:
            if str(stack.stack_name) == self.cf_stack_name:
                return stack
        return False

    def populate_params(self, current_cf_stacks):
        """
        Populate the parameter list for this stack
        """
        # If we have no parameters in the yaml file,
        # set params to an empty dict and return true
        if self.yaml_params is None:
            self.params = {}
            return True
        if self.deps_met(current_cf_stacks):
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
        elif 'value_env' in param_dict:
            var_name = str(param_dict['value_env']).strip().upper()
            if var_name in os.environ:
                return os.environ[var_name]
            else:
                raise KeyError("Cannot resolve environment variable " +
                               var_name)
        # Read value from S3 key
        elif 'value_s3' in param_dict:
            return self.get_value_from_s3(uri=param_dict['value_s3'], param=param_name)
        elif 'value_cron_timezone' in param_dict:
            return self.convert_timezone_string_to_gmt(param_dict['value_cron_timezone'])
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
                             + " for %s stack.")
            self.logger.critical(error_message, param_name, self.name)
            exit(1)

    def convert_timezone_string_to_gmt(self, cron_string):
        timezone, minute, hour, dom, month, dow = cron_string.split(' ')
        try:
            if (dom + month + dow) != '***':
                self.logger.critical("Use of dom, month and dow currently unsupported, use *")
                exit(1)
            int(minute) + int(hour)
        except ValueError:
            self.logger.critical("Please use only integers for cron hour and minute")
            exit(1)
        utcnow = datetime.datetime.utcnow()
        tz = pytz.timezone(timezone)
        offset = tz.localize(utcnow, is_dst=False).strftime('%z')
        offset_direction = offset[0]
        offset_hours = offset[1:3]
        offset_minutes = offset[-2:]

        ops = {'+': operator.sub,
               '-': operator.add}
        op_func = ops[offset_direction]

        # transform minutes
        z = op_func(int(minute), int(offset_minutes))
        if z > 60:
            new_minute = z - 60
            hour_offset = 1
        elif z < 0:
            new_minute = z + 60
            hour_offset = -1
        else:
            new_minute = z
            hour_offset = 0

        # transform hours
        z = op_func((int(hour) + int(hour_offset)), int(offset_hours))
        if z > 24:
            new_hour = z - 24
        elif z < 0:
            new_hour = z + 24
        else:
            new_hour = z

        return('{} {} {} {} {}'.format(new_minute, new_hour, dom, month, dow))

    def get_value_from_s3(self, uri, param):
      import re
      r = re.search(r"s3://(?P<bucket_name>[a-z0-9-.]+)(?P<object_path>.+)", uri)
      if not r:
        self.logger.critical("Error: invalid format for S3 lookup for parameter %s. Format is: s3://bucket-name/object/path" % param)
        exit(1)

      bucket_name = r.group('bucket_name')
      object_path = r.group('object_path')

      s3conn = boto.connect_s3()

      try:
        s3bucket = s3conn.get_bucket(bucket_name)
      except S3ResponseError:
        s3bucket = None

      if not s3bucket:
        print "Error: S3 bucket %s cannot be found or accessed" % bucket_name
        exit(1)

      s3key = s3bucket.get_key(object_path)
      if not s3key:
        print "Error: S3 object %s in bucket %s cannot be found or accessed" % (object_path, bucket_name)
        exit(1)

      return s3key.get_contents_as_string()

    def get_cf_stack(self, stack, resources=False):
        """
        Get information on parameters, outputs and resources from a stack
        and cache it
        """
        if not resources:
            if stack not in self.cf_stacks:
                # We don't have this stack in the cache already
                # so we need to pull it from CF
                cfconn = cloudformation.connect_to_region(self.region)
                self.cf_stacks[stack] = cfconn.describe_stacks(stack)[0]
            return self.cf_stacks[stack]
        else:
            if stack not in self.cf_stacks_resources:
                cfconn = cloudformation.connect_to_region(self.region)
                the_stack = self.get_cf_stack(stack=stack, resources=False)
                self.cf_stacks_resources[stack] = the_stack.list_resources()
            return self.cf_stacks_resources[stack]

    def get_value_from_cf(self, source_stack, var_type, var_name):
        """
        Get a variable from a existing cloudformation stack, var_type should be
        parameter, resource or output.
        If using resource, provide the logical ID and this will return the
        Physical ID
        """
        the_stack = self.get_cf_stack(stack=source_stack)
        if var_type == 'parameter':
            for param in the_stack.parameters:
                if str(param.key) == var_name:
                    return str(param.value)
        elif var_type == 'output':
            for output in the_stack.outputs:
                if str(output.key) == var_name:
                    return str(output.value)
        elif var_type == 'resource':
            for res in self.get_cf_stack(stack=source_stack, resources=True):
                if str(res.logical_resource_id) == var_name:
                    return str(res.physical_resource_id)
        else:
            error_message = ("Error: invalid var_type passed to" +
                             " get_value_from_cf, needs to be parameter, " +
                             "resource or output. Not: %s")
            self.logger.critical(error_message, (var_type))
            exit(1)

    def get_params_tuples(self):
        """
        Convert param dict to array of tuples needed by boto
        """
        tuple_list = []
        if len(self.params) > 0:
            for param in self.params.keys():
                tuple_list.append((param, self.params[param]))
        return tuple_list

    def read_template(self):
        """
        Open and parse the yaml/json template for this stack
        """
        try:
            template_file = open(self.template_name, 'r')
            template = yaml.load(template_file)
        except Exception as exception:
            self.logger.critical("Cannot parse %s template for stack %s."
                                 " Error: %s", self.template_name, self.name,
                                 exception)
            exit(1)
        self.template_body = simplejson.dumps(
            template,
            sort_keys=True,
            indent=0,
            separators=(',', ': '),
        )
        return True

    def template_uptodate(self, current_cf_stacks):
        """
        Check if stack is up to date with cloudformation.
        Returns true if template matches what's in cloudformation,
        false if not or stack not found.
        """
        cf_stack = self.exists_in_cf(current_cf_stacks)
        return_dict = {}
        import yaml
        if cf_stack:
            cf_temp_res = cf_stack.get_template()['GetTemplateResponse']
            cf_temp_body = cf_temp_res['GetTemplateResult']['TemplateBody']
            cf_temp_dict = yaml.load(cf_temp_body)
            old_template = yaml.load(cf_temp_body)
            new_template = yaml.load(self.template_body)
            return_dict['old'] = old_template
            return_dict['new'] = new_template
            if cf_temp_dict == yaml.load(self.template_body):
                return_dict['uptodate'] = True
                return return_dict
        return_dict['uptodate'] = False
        return return_dict

    def params_uptodate(self, current_cf_stacks):
        """
        Check if parameters in stack are up to date with Cloudformation
        """
        cf_stack = self.exists_in_cf(current_cf_stacks)
        return_dict = {}
        return_dict['old'] = cf_stack.parameters
        return_dict['old'] = dict((str(p.key), str(p.value)) for p in cf_stack.parameters)
        return_dict['new'] = self.params
        if not cf_stack:
            return_dict['uptodate'] = False
            return return_dict

        # If number of params in CF and this stack obj dont match,
        # then it needs updating
        if len(cf_stack.parameters) != len(self.params):
            msg = "New and old parameter lists are different lengths for %s"
            self.logger.debug(msg, self.name)
            return_dict['uptodate'] = False
            return return_dict

        for param in cf_stack.parameters:
            # check if param in CF exists in our new parameter set,
            # if not they are differenet and need updating
            key = param.key
            value = param.value
            if key not in self.params:
                msg = ("New params are missing key %s that exists in CF for %s"
                       + " stack already.")
                self.logger.debug(msg, key, self.name)
                return_dict['uptodate'] = False
                return return_dict
            # if the value of parameters are different, needs updating
            if self.params[key] != value:
                msg = "Param %s for stack %s has changed from %s to %s"
                self.logger.debug(msg, key, self.name,
                                  value, self.params[key])
                return_dict['uptodate'] = False
                return return_dict

        # We got to the end without returning False, so must be fine.
        return_dict['uptodate'] = True
        return return_dict
