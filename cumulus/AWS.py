"""
AWS caching module
Minimise number of calls to AWS
"""
import boto3
import logging


class CloudFormation(object):
    """
    Preform actions on aws cloudformation API
    """
    def __init__(self, region):
        logging.getLogger(__name__)
        self.conn = boto3.client('cloudformation', region_name=region)
        self.stacks = []

    def _needs_update(self, stack_name):
        """
        Mark the stack as needing update in cache
        """
        if stack_name in self.stacks:
            self.stacks[stack_name]['update_required'] = True

    def create_stack(self, stack_name, template_body, parameters,
                     tags, notification_arns=None):
        """
        Create stack in CloudFormation
        """
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
        """
        Delete the stack in CloudFormation
        """
        self._needs_update(stack_name)

        self.conn.delete_stack(StackName=stack_name)
