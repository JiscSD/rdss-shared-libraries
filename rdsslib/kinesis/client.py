import json
import logging
from .errors import MaxRetriesExceededException


class KinesisClient(object):
    def __init__(self, writer, reader, logger):
        self.logger = logger
        self.logger.setLevel(logging.INFO)
        self.writer = writer
        self.reader = reader

    def write_message(self, stream_names, payload, max_attempts):
        """Take a payload and put it into each stream in stream_names."""
        for stream_name in stream_names:
            self.writer.put_stream(stream_name, payload, max_attempts)

    def read_messages(self, stream_name, seq_number=None):
        """Continuous loop that reads messages from stream_name"""
        return self.reader.read_stream(stream_name, seq_number=seq_number)


class EnhancedKinesisClient(KinesisClient):
    def __init__(self, writer, reader, logger, error_handler, decorators=None):
        super().__init__(writer, reader, logger)
        if decorators:
            self.decorators = decorators
        else:
            self.decorators = []
        self.error_handler = error_handler

    def _apply_decorators(self, payload):
        decorated_payload = payload
        for decorator in self.decorators:
            decorated_payload = decorator.process(payload)
        return decorated_payload

    def _is_payload_json_type(self, payload):
        if type(json.loads(payload)) is not dict:
            return False
        else:
            return True

    def write_message(self, stream_names, payload, max_attempts):
        if self._is_payload_json_type(payload):
            decorated_payload = self._apply_decorators(payload)
            if decorated_payload:
                try:
                    super().write_message(stream_names, payload, max_attempts)
                except MaxRetriesExceededException as e:
                    stream_name = e.args[0]
                    error_code = 'GENERR005'
                    error_description = 'Maximum retry attempts [%s] exceed for stream [%s]' % (
                        max_attempts, stream_name)
                    self.error_handler.handle_error(
                        payload, error_code, error_description)
            else:
                # payload decoration has failed - move to invalid stream
                self.error_handler.handle_invalid_json(payload)
        else:
            self.error_handler.handle_invalid_json(payload)
