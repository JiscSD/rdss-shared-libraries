import json
import logging

from .errors import MaxRetriesExceededException, DecoratorApplyException


MAX_ATTEMPTS = 6


class KinesisClient(object):
    def __init__(self, writer, reader):
        """
        Writes and reads messages to and from Kinesis streams
        :param writer: handles writing of payloads to Kinesis stream
        :param reader: handles reading of payloads from Kinesis stream
        :type writer: writer.StreamWriter
        :type reader: reader.StreamReader
        """
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        self.writer = writer
        self.reader = reader

    def write_message(self, stream_names, payload, max_attempts=MAX_ATTEMPTS):
        """Write a payload into each stream in stream_names
        :param stream_names: Kinesis streams to write to
        :param payload:  JSON payload
        :param max_attempts: maximum number of times to attempt writing
        :type stream_names: list of str
        :type payload: str
        """
        for stream_name in stream_names:
            self.writer.put_stream(stream_name, payload, max_attempts)

    def read_messages(self, stream_name, seq_number=None):
        """Continuous loop that reads messages from stream_name
        :param stream_name: Name of Kinesis stream to read from
        :param seq_number: Optional seq number
        :type stream_name: str
        :return message_gen: Yields messages read from Kinesis stream
        :rtype message_gen: generator
        """
        message_gen = self.reader.read_stream(
            stream_name, seq_number=seq_number)
        return message_gen


class EnhancedKinesisClient(KinesisClient):
    def __init__(self, writer, reader, error_handler, decorators=None):
        """
        Writes and reads messages to and from Kinesis streams with
        error handling and message decoration
        :param writer: Writes messages to Kinesis stream
        :param reader: Reads messages from Kinesis stream
        :param error_handler: Handles messages with errors
        :param decorators: Enhance messages with extra fields
        :type writer: writer.StreamWriter
        :type reader: reader.StreamReader
        :type error_handler: handlers.MessageErrorHandler
        :type decorators: list
        """
        super().__init__(writer, reader)
        if decorators:
            self.decorators = decorators
        else:
            self.decorators = []
        self.error_handler = error_handler

    def _apply_decorators(self, payload):
        """
        Applies a sequence of decorators that
        enhance and modify the contents of a payload
        :param payload: Undecorated JSON payload
        :type payload: str
        :return payload: Decorated JSON payload
        :rtype payload: str
        """
        decorated_payload = payload
        for decorator in self.decorators:
            try:
                decorated_payload = decorator.process(payload)
            except Exception:
                self.logger.warning(
                    'Failed to apply decorator {}'.format(decorator.name))
                raise DecoratorApplyException()
        return decorated_payload

    def write_message(self, stream_names, payload, max_attempts=MAX_ATTEMPTS):
        """Write a payload into each stream in stream_names
        :param stream_names: Kinesis streams to write to
        :param payload: JSON payload
        :param max_attempts: Max number of times to attempt writing
        :type stream_names: list of str
        :type payload: str
        :type max_attempts: int
        """
        try:
            json.loads(payload)
        except json.decoder.JSONDecodeError:
            self.error_handler.handle_invalid_json(payload)
            return

        decorated_payload = self._apply_decorators(payload)
        for stream_name in stream_names:
            try:
                super().write_message([stream_name],
                                      decorated_payload,
                                      max_attempts)
            except MaxRetriesExceededException as e:
                stream_name = e.args[0]
                error_code = 'GENERR005'
                error_description = 'Maximum retry attempts {0} exceed'\
                    'for stream {1}'.format(max_attempts,
                                            stream_name)
                self.error_handler.handle_error(decorated_payload,
                                                error_code,
                                                error_description)

    def handle_error(self, payload, error_code, error_description):
        """ Allows errors to be posted to the stream occurring from
        activities like payload validation
        :param payload: JSON payload
        :param error_code: Error Code
        :param error_description: Description Of Error
        """
        self.error_handler.handle_error(payload, error_code, error_description)
