""" Factory for creating Kinesis Clients"""
import boto3

from .client import KinesisClient, EnhancedKinesisClient
from .decorators import RouterHistoryDecorator
from .handlers import MessageErrorHandler
from .reader import StreamReader
from .writer import StreamWriter


def kinesis_client_factory(client_app):
    boto_client = boto3.client('kinesis')
    writer = StreamWriter(client=boto_client)
    reader = StreamReader(client=boto_client)

    if client_app == 'basic':
        return KinesisClient(writer=writer,
                             reader=reader)
    elif client_app == 'enhanced':
        decorators = [RouterHistoryDecorator()]
        handler = MessageErrorHandler(invalid_stream_name='invalid_stream',
                                      error_stream_name='error_stream',
                                      writer=writer)
        return EnhancedKinesisClient(writer=writer,
                                     reader=reader,
                                     error_handler=handler,
                                     decorators=decorators)
