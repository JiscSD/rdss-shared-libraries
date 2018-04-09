import json
import pytest
from rdsslib.kinesis import decorators


class TestHistoryDecorator(object):

    def setup(self):
        self.decorator = decorators.RouterHistoryDecorator()

    @pytest.fixture
    def payload(self):
        """Return a sample payload."""
        return {
            'messageHeader': {
                'id': '90cbdf86-6892-4bf9-845f-dbd61eb80065'
            },
            'messageBody': {
                'some': 'message'
            }
        }

    @pytest.fixture
    def serialised_payload(self, payload):
        """Return serialised payload"""
        return json.dumps(payload)

    def test_history_decorator(self, serialised_payload):
        decorated_payload = self.decorator.process(
            serialised_payload, 'some-id')
        json_payload = json.loads(decorated_payload)
        msg_history = json_payload['messageHeader']['messageHistory'][0]
        for element in ['machineId', 'timestamp', 'machineAddress']:
            assert element in msg_history

    def test_history_decorator_when_header_is_string(self, serialised_payload):
        invalid_message = {
            'messageHeader': 'should-be-dict'
        }
        decorated_payload = self.decorator.process(
            json.dumps(invalid_message), 'some-id')
        json_payload = json.loads(decorated_payload)
        msg_history = json_payload['messageHeader']['messageHistory'][0]
        for element in ['machineId', 'timestamp', 'machineAddress']:
            assert element in msg_history
