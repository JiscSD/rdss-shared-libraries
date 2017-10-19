from collections import defaultdict
import json
import moto
import pytest


class MockStreamWriter(object):
    def __init__(self):
        self.streams = defaultdict(list)

    def put_stream(self, stream_name, payload, max_attempts):
        self.streams[stream_name].append(payload)


class KinesisMixin(object):

    def setup(self):
        self.mock = moto.mock_kinesis()
        self.mock.start()

    @pytest.fixture
    def payload(self):
        """Return a sample payload."""
        return {
            'messageHeader': {
                'id': '90cbdf86-6892-4bf9-845f-dbd61eb80065'
            },
            'messageBody': {
                'some': 'message'
            }
        }

    @pytest.fixture
    def serialised_payload(self, payload):
        """Return payload serialised to JSON formatted str"""
        return json.dumps(payload)

    def teardown(self):
        """Stop mocking kinesis."""
        self.mock.stop()


def get_records(client, stream_name):
    """Return a list of records from given stream."""
    shard_id = client.describe_stream(
        StreamName=stream_name
    )['StreamDescription']['Shards'][0]['ShardId']
    shard_iterator = client.get_shard_iterator(
        StreamName=stream_name,
        ShardId=shard_id,
        ShardIteratorType='TRIM_HORIZON'
    )['ShardIterator']
    result = client.get_records(
        ShardIterator=shard_iterator,
        Limit=1000
    )
    return result['Records']