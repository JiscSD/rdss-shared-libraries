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
    def __init__(self, writer, reader, logger, decorator, error_handler):
        super().__init__(writer, reader, logger)
        self.decorator = decorator
        self.error_handler = error_handler


    def _decorate_message_history(self, payload):
        return self.decorator.process(payload)

    def _check_payload_json_type(self, payload):
        if type(json.loads(payload)) is not dict:
            return False
        else:
            return True

    def write_message(self, stream_names, payload, max_attempts):
        if self._check_payload_json_type(payload):
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
                # payload decoration has failed - move to invalid stream
                self.error_handler.handle_invalid_json(payload)
        else:
            self.error_handler.handle_invalid_json(payload)