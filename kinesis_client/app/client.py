""" Kinesis Client object for writing and reading to and from a Kinesis Stream """


import boto3
from .errors import MaxRetriesExceededException
import logging
import time

class KinesisClient(object):
    def __init__(self):
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)
        self.client = boto3.client('kinesis')

    def write_message(self, stream_names, payload, max_attempts):
        """Take a payload and put it into each stram in stream_names."""
        for stream_name in stream_names:
            self.__put_record(stream_name, payload, max_attempts)

    def __put_record(self, stream_name, payload, max_attempts):
        """Attempt to put the payload in the provided stream name."""
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
        raise MaxRetriesExceededException()

    def __do_put_record(self, stream_name, payload, attempt):
        """Put the payload in the provided stream."""
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


    def read_messages(self):
        """ Read messages from Kinesis stream"""
        pass
