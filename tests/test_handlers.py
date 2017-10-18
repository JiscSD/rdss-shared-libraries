from collections import defaultdict
import json
import mock
import pytest
from rdsslib.kinesis import handlers


class MockStreamWriter(object):
    def __init__(self):
        self.streams = defaultdict(list)

    def put_stream(self, stream_name, payload, max_attempts):
        self.streams[stream_name].append(payload)


class TestMessageErrorHandler(object):

    def setup(self):
        self.logger = mock.Mock()
        self.mock_writer = MockStreamWriter()
        self.handler = handlers.MessageErrorHandler(
            invalid_stream_name="invalid_stream",
            error_stream_name="error_stream",
            logger=self.logger,
            stream_writer=self.mock_writer
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
        self.handler.handle_error(serialised_payload, "ERROR", "Error occurred")
        payload = self.mock_writer.streams['error_stream'][0]
        assert json.loads(payload)['messageBody'] == {'some': 'message'}


