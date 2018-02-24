import logging
import time

from .errors import MaxRetriesExceededException


class StreamWriter(object):
    """ Writes payloads to a Kinesis stream"""

    def __init__(self, client):
        """
        :param client: Boto3 Kinesis client
        :type client: botocore.client.Kinesis
        """
        self.client = client
        self.logger = logging.getLogger(__name__)

    def put_stream(self, stream_name, payload, max_attempts):
        """
        Attempt to put the payload in the provided
        stream name for max_attempts.
        :param stream_name: Name of Kinesis stream
        :param payload: JSON payload
        :param max_attempts: Maximum number of times to try
        :return:
        """
        attempt = 1

        while attempt <= max_attempts:
            if self.__do_put_record(stream_name, payload, attempt):
                return
            else:
                sleep_seconds = pow(2, attempt) / 10
                self.logger.info(
                    'Backing off for [%s] seconds before '
                    'attempt [%s]/[%s]',
                    sleep_seconds, attempt, max_attempts)
                time.sleep(sleep_seconds)
                attempt += 1

        self.logger.error(
            'GENERR005 Unable to add payload [%s] to Kinesis Stream [%s] '
            '- maximum retries exceeded.', payload, stream_name)
        raise MaxRetriesExceededException(stream_name)

    def __do_put_record(self, stream_name, payload, attempt):
        """Put the payload in the provided stream.
        :param stream_name: Name of Kinesis stream
        :param payload: JSON formatted payload
        :param attempt: The order of this write attempt in the sequence
                        of previous writes
        :return: True or False depending on the result of write attempt
        :rtype: bool
        """
        self.logger.info(
            'Executing attempt [%s] at adding payload [%s] to Kinesis Stream '
            '[%s]', attempt, payload, stream_name)

        try:
            self.client.put_record(
                StreamName=stream_name,
                Data=payload,
                PartitionKey=str(int(time.time() * 1000))
            )
            return True
        except Exception:
            self.logger.exception(
                'GENERR006 An error occured adding payload [%s] to Kinesis '
                'Stream [%s].',
                payload, stream_name)
            return False
