""" Error handling and exceptions for shared Kinesis client"""


class KinesisClientException(Exception):
    pass


class MaxRetriesExceededException(KinesisClientException):
    pass


class DecoratorApplyException(KinesisClientException):
    pass
