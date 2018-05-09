# -*- coding: UTF-8 -*-
"""
    S3 Client
    ~~~~~~~~~

    Client wrapper around boto3 library for interacting with S3.
"""
import boto3
import base64
import hashlib


class S3Client(object):
    """ Client wrapper around Boto3."""

    def __init__(self):
        self.client = boto3.client('s3')

    def _generate_md5_checksum(self, content):
        """ Generates a md5 checksum for inclusion in upload to s3.
            """
        md5_checksum = hashlib.md5(content)
        return base64.b64encode(md5_checksum.digest()).decode('utf-8')

    def save_str_file(self, contents, bucket, key, content_type='text/html'):
        """ Upload a text document to a file on S3.
        :param contents: String with contents to store in file
        :param bucket: String with name of bucket to store data in
        :param key: String with key name for the object to store
        :param content_type: String with MIME type of file"""
        contents_str = contents.encode('utf-8')
        md5_checksum = self._generate_md5_checksum(contents_str)
        self.client.put_object(
            Body=contents_str,
            Bucket=bucket,
            Key=key,
            ContentType=content_type,
            ContentMD5=md5_checksum,
            Metadata={
                'md5chksum': md5_checksum
                }
        )
