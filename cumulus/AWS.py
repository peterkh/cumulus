"""
AWS caching module.

Minimise number of calls to AWS
"""
import boto3
import logging
import cumulus.Exception


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
    DELETED_STATUS = ['DELETE_COMPLETE']

    def __init__(self, region):
        """Set connection to CloudFomration API."""
        self.logger = logging.getLogger(__name__)
        self.conn = boto3.client('cloudformation', region_name=region)

        # Stack summary list, update on any changes to any stacks
        self.stack_summaries = {}
        self.stack_summaries_updated = False
        self._refresh_stack_list()

        # Detailed stack dict, update per stack when needed
        self.stacks = {}

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

    def describe_stack(self, stack_name):
        """Return stack details from CloudFormation."""
        if self.exists(stack_name):
            if stack_name in self.stacks:
                if self.stacks[stack_name]['updated']:
                    return self.stacks[stack_name]['details']
            self.logger.info('Updating stack details for %s', stack_name)
            details = self.conn.describe_stack(StackName=stack_name)
            self.stacks[stack_name] = {'updated': True, 'details': details}
            return self.stacks[stack_name]['details']
        else:
            raise cumulus.Exception.StackDoesNotExist(
                'Can not retrieve details for no existant stack')

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
