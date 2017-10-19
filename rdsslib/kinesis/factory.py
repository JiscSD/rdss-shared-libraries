""" Factory for creating Kinesis Clients"""
import boto3
import logging
from .client import KinesisClient, EnhancedKinesisClient
from .decorators import RouterHistoryDecorator
from .handlers import MessageErrorHandler
from .reader import StreamReader
from .writer import StreamWriter


def kinesis_client_factory(client_app):
    boto_client = boto3.client('kinesis')
    logger = logging.getLogger()
    writer = StreamWriter(client=boto_client,
                          logger=logger)
    reader = StreamReader(client=boto_client)

    if client_app == 'basic':
        return KinesisClient(writer=writer,
                             reader=reader,
                             logger=logger)
    elif client_app == 'enhanced':
        decorators = [RouterHistoryDecorator()]
        handler = MessageErrorHandler(invalid_stream_name='invalid_stream',
                                      error_stream_name='error_stream',
                                      logger=logger,
                                      writer=writer)
        return EnhancedKinesisClient(writer=writer,
                                     reader=reader,
                                     logger=logger,
                                     error_handler=handler,
                                     decorators=decorators)
