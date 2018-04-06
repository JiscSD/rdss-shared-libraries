""" Factory for creating Kinesis Clients"""
import boto3

from .client import KinesisClient, EnhancedKinesisClient
from .decorators import RouterHistoryDecorator
from .handlers import MessageErrorHandler
from .reader import StreamReader
from .writer import StreamWriter


def kinesis_client_factory(
        client_type,
        machine_id=None,
        invalid_stream_name='invalid_stream',
        error_stream_name='error_stream',
        read_interval=0.2):
    """ Create customised instances of KinesisClient or its subclasses
    :param client_type: Specifies the type of client that the factory
                        should construct
    :return: An instance of Kinesis client
    :rtype: client.KinesisClient or client.EnhancedKinesisClient
    """
    boto_client = boto3.client('kinesis')
    writer = StreamWriter(client=boto_client)
    reader = StreamReader(client=boto_client, read_interval=read_interval)

    if client_type == 'basic':
        return KinesisClient(writer=writer,
                             reader=reader)
    elif client_type == 'enhanced':
        decorators = [RouterHistoryDecorator()]
        handler = MessageErrorHandler(invalid_stream_name=invalid_stream_name,
                                      error_stream_name=error_stream_name,
                                      writer=writer)
        return EnhancedKinesisClient(writer=writer,
                                     reader=reader,
                                     machine_id=machine_id,
                                     error_handler=handler,
                                     decorators=decorators)
