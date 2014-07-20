import cumulus
import mock
import unittest

class TestCFStack(unittest.TestCase):

    def test_exit_on_template_open_fail(self):
        with mock.patch('__builtin__.open') as mock_open:
            mock_open.side_effect=SystemExit(1)
            
            with self.assertRaises(SystemExit) as context:
                myStack = cumulus.CFStack.CFStack("test", "stack1", {"param1": 1}, "stack.template", "ap-southeast-2", None)
            self.assertEqual(context.exception.code, 1)

    def test_exit_on_bad_params(self):
        with mock.patch('__builtin__.open') as mock_open:
            mock_open.return_value = mock.MagicMock(spec=file)
            with self.assertRaises(SystemExit) as context:
                myStack = cumulus.CFStack.CFStack("test", "stack1", "These are not the params you are looking for", "stack.template", "ap-southeast-2", None)
            self.assertEqual(context.exception.code, 1)

    def test_create_cfstack_object(self):
        with mock.patch('__builtin__.open') as mock_open:
            mock_open.return_value = mock.MagicMock(spec=file)
            myStack = cumulus.CFStack.CFStack("test", "stack1", {"param1": 1}, "stack.template", "ap-southeast-2", None)
        self.assertEqual(myStack.tags, {})

