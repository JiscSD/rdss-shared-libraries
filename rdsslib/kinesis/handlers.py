import json


class MessageErrorHandler(object):
    """ Handles invalid and errored messages"""
    def __init__(self, invalid_stream_name, error_stream_name, logger, writer):
        self.invalid_stream_name = invalid_stream_name
        self.error_stream_name = error_stream_name
        self.logger = logger
        self.writer = writer

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
                self.handle_invalid_json(payload_json)
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

            self.writer.put_stream(self.error_stream_name, payload, 1)
        except:
            self.logger.exception(
                'Unable to move payload [%s] to stream [%s]',
                payload,
                self.error_stream_name
            )

    def handle_invalid_json(self, payload):
        try:
            self.logger.info(
                'Moving invalid JSON payload [%s] to stream [%s]',
                payload,
                self.invalid_stream_name
            )
            self.writer.put_stream(self.invalid_stream_name, payload, 1)
        except:
            self.logger.exception(
                'Unable to move payload [%s] to stream [%s]',
                payload,
                self.invalid_stream_name
            )
