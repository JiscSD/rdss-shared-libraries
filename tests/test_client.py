"""Tests for Kinesis Client """

import pytest
import moto
from rdsslib.kinesis import client


class TestKinesisClient(object):
    """Test the Kinesis Client."""

    def setup(self):
        """Start mocking kinesis."""
        self.mock = moto.mock_kinesis()
        self.mock.start()

    @pytest.fixture
    def kc(self):
        return client.KinesisClient()

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

    def test_raises_max_retries_exceeded(self, payload, kc):
        """Test that client raises MaxRetriesExceededException."""
        with pytest.raises(client.MaxRetriesExceededException):
            kc.write_message(['no_stream'], payload, 1)

    def teardown(self):
        """Stop mocking kinesis."""
        self.mock.stop()