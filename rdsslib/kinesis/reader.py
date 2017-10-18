import boto3
import time


class StreamReader(object):
    def __init__(self):
        self.client = boto3.client('kinesis')

    def read_stream(self, stream_name, seq_number=None):
        """Listen for messages from a stream and yield response."""
        shard_id = self._get_shard_id(stream_name)
        iterator = self._get_shard_iterator(
            stream_name,
            shard_id,
            seq_number
        )
        while True:
            response = self.client.get_records(
                ShardIterator=iterator,
                Limit=100
            )
            records = response['Records']
            for record in records:
                yield record

            time.sleep(0.2)
            iterator = response['NextShardIterator']

    def _get_shard_id(self, stream_name):
        """Get the shard ID for a given stream name."""
        response = self.client.describe_stream(
            StreamName=stream_name
        )
        return response['StreamDescription']['Shards'][0]['ShardId']

    def _get_shard_iterator(self, stream_name, shard_id, seq_number):
        """Get the initial shard iterator."""
        if not seq_number:
            response = self.client.get_shard_iterator(
                StreamName=stream_name,
                ShardId=shard_id,
                ShardIteratorType='TRIM_HORIZON'
            )
        else:
            response = self.client.get_shard_iterator(
                StreamName=stream_name,
                ShardId=shard_id,
                ShardIteratorType='AT_SEQUENCE_NUMBER',
                StartingSequenceNumber=seq_number
            )
        return response['ShardIterator']