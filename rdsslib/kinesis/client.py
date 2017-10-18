import boto3
import json
import logging
from .errors import MaxRetriesExceededException
from .writer import StreamWriter
from .reader import StreamReader


class KinesisClient(object):
    def __init__(self):
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)
        self.client = boto3.client('kinesis')
        self.writer = StreamWriter(client = self.client, logger=self.logger)
        self.reader = StreamReader(client=self.client)

    def write_message(self, stream_names, payload, max_attempts):
        """Take a payload and put it into each stream in stream_names."""
        for stream_name in stream_names:
            self.writer.put_stream(stream_name, payload, max_attempts)

    def read_messages(self, stream_name, seq_number=None):
        """Continuous loop that reads messages from stream_name"""
        return self.reader.read_stream(stream_name, seq_number=seq_number)


class EnhancedKinesisClient(KinesisClient):
    def __init__(self, decorator, error_handler):
        super().__init__()
        self.decorator = decorator
        self.error_handler = error_handler

    def _decorate_message_history(self, payload):
        return self.decorator.process(payload)

    def _check_payload_json_type(self, payload):
        if type(json.loads(payload)) is not dict:
            self.error_handler.handle_invalid_json(payload)

    def write_message(self, stream_names, payload, max_attempts):
        self._check_payload_json_type(payload)
        decorated_payload = self._decorate_message_history(payload)
        if decorated_payload:
            try:
                super().write_message(stream_names, payload, max_attempts)
            except MaxRetriesExceededException as e:
                stream_name = e.args[0]
                error_code = 'GENERR005'
                error_description = 'Maximum retry attempts [%s] exceed for stream [%s]' % (max_attempts, stream_name)
                self.error_handler.handle_error(payload, error_code, error_description)
        else:
            self.error_handler.handle_invalid_json(payload)