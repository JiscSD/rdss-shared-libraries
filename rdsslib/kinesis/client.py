import boto3
import logging
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