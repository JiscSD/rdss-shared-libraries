import time


class StreamReader(object):
    def __init__(self, client, read_interval):
        """
        Boto3 abstraction that reads from a Kinesis stream
        :param client: An instance of boto3 kinesis client
        :type client: botocore.client.Kinesis
        """
        self.client = client
        self.read_interval = read_interval

    def read_stream(self, stream_name, seq_number=None):
        """Listen for messages from a stream and yield response.
        :param stream_name: name of the Kinesis stream
        :param seq_number: Kinesis sequence number
        :type stream_name: str
        :type seq_number: int
        :return: A generator that yields messages
        :rtype: generator
        """
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

            time.sleep(self.read_interval)
            iterator = response['NextShardIterator']

    def _get_shard_id(self, stream_name):
        """Get the shard ID for a given stream name.
        :param stream_name:
        :return: Shard id of Kinesis stream
        :rtype: str
        """
        response = self.client.describe_stream(
            StreamName=stream_name
        )
        return response['StreamDescription']['Shards'][0]['ShardId']

    def _get_shard_iterator(self, stream_name, shard_id, seq_number):
        """Get the initial shard iterator.
        :param stream_name: Name of Kinesis stream
        :param shard_id:  Shard id
        :param seq_number: Sequence number of message
        :return: Shard iterator
        :rtype: str
        """
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
                ShardIteratorType='AFTER_SEQUENCE_NUMBER',
                StartingSequenceNumber=seq_number
            )
        return response['ShardIterator']
