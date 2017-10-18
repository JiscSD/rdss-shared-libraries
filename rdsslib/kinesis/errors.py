""" Error handling and exceptions for shared Kinesis client"""
import json
from .streamio import StreamWriter

class MaxRetriesExceededException(Exception):
    pass


class MessageErrorHandler(object):
    """ Handles invalid and errored messages"""
    def __init__(self, invalid_stream_name, error_stream_name, logger):
        self.invalid_stream_name = invalid_stream_name
        self.logger = logger
        self.writer = StreamWriter()

    def handle_error(self, payload, error_code, error_description):
        try:
            self.logger.info(
                'Setting \'errorCode\' [%s] and \'errorDescription\' [%s] on '
                'payload [%s]',
                error_code,
                error_description,
                payload
            )

            try:
                payload_json = json.loads(payload)
            except ValueError:
                self._handle_invalid_msg(payload_json)
                return

            payload_json['messageHeader']['errorCode'] = error_code
            payload_json['messageHeader'][
                'errorDescription'] = error_description
            payload = json.dumps(payload_json)

            self.logger.info(
                'Moving erroneous payload [%s] to stream [%s] with code [%s] '
                'and description [%s]',
                payload,
                self.error_stream_name,
                error_code,
                error_description
            )

            self.writer.write(self.error_stream_name, payload, 1)
        except:
            self.logger.exception(
                'Unable to move payload [%s] to stream [%s]',
                payload,
                self.error_stream_name
            )

    def _handle_invalid_msg(self, payload):
        try:
            self.logger.info(
                'Moving invalid JSON payload [%s] to stream [%s]',
                payload,
                self.invalid_stream_name
            )
            self.writer.write(self.invalid_stream_name, payload, 1)
        except:
            self.logger.exception(
                'Unable to move payload [%s] to stream [%s]',
                payload,
                self.invalid_stream_name
            )
