"""
AWS caching module.

Minimise number of calls to AWS
"""
import boto3
import logging
import cumulus.Exception


class StackList(dict):
    """Simple dict class that returns a dict as default for any key."""

    def __missing__(self, key):
        """If a key is missing, set and return a dict."""
        self.__setitem__(key, {})
        return self.__getitem__(key)


class CloudFormation(object):
    """Preform actions on aws cloudformation API."""

    # All status possible, except DELETE_COMPLETE
    EXISTING_STATUS = ['CREATE_IN_PROGRESS',
                       'CREATE_FAILED',
                       'CREATE_COMPLETE',
                       'ROLLBACK_IN_PROGRESS',
                       'ROLLBACK_FAILED',
                       'ROLLBACK_COMPLETE',
                       'DELETE_IN_PROGRESS',
                       'DELETE_FAILED',
                       'UPDATE_IN_PROGRESS',
                       'UPDATE_COMPLETE_CLEANUP_IN_PROGRESS',
                       'UPDATE_COMPLETE',
                       'UPDATE_ROLLBACK_IN_PROGRESS',
                       'UPDATE_ROLLBACK_FAILED',
                       'UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS',
                       'UPDATE_ROLLBACK_COMPLETE', ]
    # Status for deleted stacks
    DELETED_STATUS = ['DELETE_COMPLETE']
    # Status for a valid existing stack (can I depend on it)
    VALID_STATUS = ['CREATE_COMPLETE',
                    'ROLLBACK_IN_PROGRESS',
                    'ROLLBACK_COMPLETE',
                    'UPDATE_IN_PROGRESS',
                    'UPDATE_COMPLETE_CLEANUP_IN_PROGRESS',
                    'UPDATE_COMPLETE',
                    'UPDATE_ROLLBACK_IN_PROGRESS',
                    'UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS',
                    'UPDATE_ROLLBACK_COMPLETE', ]

    def __init__(self, region):
        """Set connection to CloudFomration API."""
        self.logger = logging.getLogger(__name__)
        self.conn = boto3.client('cloudformation', region_name=region)

        # Stack summary list, update on any changes to any stacks
        self.stack_summaries = {}
        self.stack_summaries_updated = False
        self._refresh_stack_list()

        # Detailed stack dict, update per stack when needed
        self.stacks = StackList()

    def _list_stacks(self):
        """Get list of stacks from CloudFormation."""
        stacks = []
        response = self.conn.list_stacks()
        stacks.extend(response['StackSummaries'])
        while 'NextToken' in response:
            self.logger.info('Processing NextToken...')
            response = self.conn.list_stacks(
                NextToken=response['NextToken'])
            stacks.extend(response['StackSummaries'])
        return stacks

    def list_existing_stacks(self):
        """Get stack list from CloudFormation if not already updated."""
        if not self.stack_summaries_updated:
            self._refresh_stack_list()
        return {stack: props for stack, props in
                self.stack_summaries.iteritems()
                if props['status'] in self.VALID_STATUS}

    def _list_stack_resources(self, stack_name):
        """Get list of stack resources from CloudFormation."""
        resources = []
        response = self.conn.list_stack_resources(StackName=stack_name)
        resources.extend(response['StackResourceSummaries'])
        while 'NextToken' in response:
            self.logger.info('Processing NextToken...')
            response = self.conn.list_stack_resources(
                StackName=stack_name,
                NextToken=response['NextToken'])
            resources.extend(response['StackResourceSummaries'])
        return resources

    def _refresh_stack_list(self):
        """Update the stack summary cache from CloudFormation."""
        # Clear out old summaries
        self.stack_summaries = {}

        # Get the stack summaries in CloudFormation
        stacks = self._list_stacks()

        for stack in stacks:
            stack_name = stack['StackName']
            if stack_name not in self.stack_summaries:
                self.stack_summaries[stack_name] = {}

            if ('status' not in self.stack_summaries[stack_name] or
                    stack['StackStatus'] not in self.DELETED_STATUS):
                self.stack_summaries[stack_name]['status'] \
                    = stack['StackStatus']

        self.stack_summaries_updated = True

    def _needs_update(self, stack_name):
        """Mark the stack as needing update in cache."""
        self.stack_summaries_updated = False
        if stack_name in self.stacks:
            self.stacks[stack_name]['updated'] = False
            self.stacks[stack_name]['resources_updated'] = False
            self.stacks[stack_name]['template_updated'] = False

    def exists(self, stack_name):
        """Check if stack exists in CloudFormation currently."""
        if not self.stack_summaries_updated:
            self._refresh_stack_list()

        if stack_name in self.stack_summaries:
            if (self.stack_summaries[stack_name]['status'] not in
                    self.DELETED_STATUS):
                return True
        else:
            return False

    def get_stack_status(self, stack_name):
        """Get the status of a stack in CloudFormation."""
        if self.exists(stack_name):
            return {stack_name: self.stack_summaries[stack_name]}
        else:
            raise cumulus.Exception.StackDoesNotExist(
                'Can not retrieve status for non-existant stack')

    def describe_stack(self, stack_name, resources=False):
        """Return stack details from CloudFormation."""
        if not self.exists(stack_name):
            raise cumulus.Exception.StackDoesNotExist(
                'Can not retrieve details for non-existant stack')

        if self._details_uptodate(stack_name):
            details = self.stacks[stack_name]['details']
        else:
            self.logger.info('Updating stack details for %s', stack_name)
            details = self.conn.describe_stacks(
                StackName=stack_name)['Stacks'][0]
            self.stacks[stack_name]['details'] = details
            self._details_updated(stack_name)

        # If we request the resources and they arn't up to date,
        # go get them
        if resources and not self._resources_uptodate(stack_name):
            details['resources'] = self._list_stack_resources(stack_name)
            self.stacks[stack_name]['details'] = details
            self._resources_updated(stack_name)
        return details

    def get_template(self, stack_name):
        """Return json template for CloudFormation stack."""
        if not self.exists(stack_name):
            raise cumulus.Exception.StackDoesNotExist(
                'Can not retrieve details for non-existant stack')

        if self._template_uptodate(stack_name):
            return self.stacks[stack_name]['template']
        else:
            self.logger.info('Downloading stack template from CloudFormation'
                             ' for %s', stack_name)
            response = self.conn.get_template(StackName=stack_name)
            return response['TemplateBody']

    def _details_uptodate(self, stack_name):
        """Check if stack_name details are up to date in cache."""
        return self.stacks[stack_name].get('updated', False)

    def _resources_uptodate(self, stack_name):
        """Check if stack_name resources are up to date in cache."""
        return self.stacks[stack_name].get('resources_updated', False)

    def _template_uptodate(self, stack_name):
        """Check if stack_name template is up to date in cache."""
        return self.stacks[stack_name].get('template_updated', False)

    def _details_updated(self, stack_name):
        """Mark stack_name details as up to date in cache."""
        self.stacks[stack_name]['updated'] = True

    def _resources_updated(self, stack_name):
        """Mark stack_name resources as up to date in cache."""
        self.stacks[stack_name]['resources_updated'] = True

    def _template_updated(self, stack_name):
        """Mark stack_name template as up to date in cache."""
        self.stacks[stack_name]['template_updated'] = True

    def create_stack(self, stack_name, template_body, parameters,
                     tags, notification_arns=None):
        """Create stack_name in CloudFormation."""
        if not notification_arns:
            notification_arns = []

        self._needs_update(stack_name)

        # create the stack
        self.conn.create_stack(
            StackName=stack_name,
            TemplateBody=template_body,
            Parameters=parameters,
            Capabilities=['CAPABILITY_IAM'],
            NotificationARNs=notification_arns,
            Tags=tags)

    def delete_stack(self, stack_name):
        """Delete stack_name in CloudFormation."""
        self._needs_update(stack_name)
        self.conn.delete_stack(StackName=stack_name)

    def update_stack(self, **kwargs):
        """Update stack_name in CloudFormation."""
        if 'StackName' not in kwargs:
            raise cumulus.Exception.MissingAWSParameter(
                'Update stack requires StackName')
        self._needs_update(kwargs['StackName'])

        self.conn.update_stack(**kwargs)
