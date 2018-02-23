import json

from rdsslib.kinesis.client import EnhancedKinesisClient

from .kinesis_helpers import MockStreamWriter
import pytest
from rdsslib.kinesis import handlers


class TestMessageErrorHandler(object):

    def setup(self):
        self.mock_writer = MockStreamWriter()
        self.handler = handlers.MessageErrorHandler(
            invalid_stream_name='invalid_stream',
            error_stream_name='error_stream',
            writer=self.mock_writer
        )

    @pytest.fixture
    def serialised_payload(self):
        return json.dumps({
            'messageHeader': {
                'id': '90cbdf86-6892-4bf9-845f-dbd61eb80065'
            },
            'messageBody': {
                'some': 'message'
            }
        })

    def test_invalid_json_handling(self, serialised_payload):
        self.handler.handle_invalid_json(serialised_payload)
        payload = self.mock_writer.streams['invalid_stream'][0]
        assert json.loads(payload)['messageBody'] == {'some': 'message'}

    def test_error_handling_with_valid_json(self, serialised_payload):
        self.handler.handle_error(
            serialised_payload, 'ERROR', 'Error occurred')
        payload = self.mock_writer.streams['error_stream'][0]
        assert json.loads(payload)['messageBody'] == {'some': 'message'}

    def test_error_handling_from_client_with_valid_json(self, serialised_payload):
        mock_client = EnhancedKinesisClient(None, None, self.handler, None)
        mock_client.handle_error(payload=serialised_payload,
                                 error_code='TESTERR001',
                                 error_description='Generated Test Message to Error')
        payload = self.mock_writer.streams['error_stream'][0]
        assert json.loads(payload)['messageBody'] == {'some': 'message'}