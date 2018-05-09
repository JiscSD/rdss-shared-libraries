import pytest
import moto
import boto3

from rdsslib.s3.client import S3Client


@pytest.fixture()
def client():
    return S3Client()


@pytest.fixture()
def boto3_client():
    return boto3.client('s3')

@pytest.fixture()
def sample_text():
    return 'sample'

@pytest.fixture()
def sample_md5():
    return 'Xo/5v1W6NQgZnSLphBKb5g=='

def create_test_bucket(name='test'):
    """ Create a test S3 bucket."""
    boto3_client().create_bucket(Bucket=name)
    return name


@moto.mock_s3()
def test_put_bucket_object(client, boto3_client, sample_text, sample_md5):
    """ Test putting an object in an S3 bucket."""
    test_bucket = create_test_bucket()
    client.save_str_file(sample_text, test_bucket, 'sample.txt', 'text/plain')

    boto3_client = boto3.client('s3')
    item = boto3_client.get_object(
        Bucket=test_bucket,
        Key='sample.txt'
    )
    response_md5 = item['ResponseMetadata']['HTTPHeaders']['Content-MD5']

    assert item['Body'].read().decode('utf-8') == sample_text
    assert response_md5 == sample_md5
