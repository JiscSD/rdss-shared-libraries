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