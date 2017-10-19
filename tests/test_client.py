"""Tests for Kinesis Client """


import json
from .kinesis_helpers import KinesisMixin, MockStreamWriter
import mock
import pytest
from rdsslib.kinesis import client, decorators, handlers, writer, reader, factory


class TestKinesisClient(KinesisMixin):
    """Test the Kinesis Client."""

    def test_client_writes_and_reads_messages(self, serialised_payload):
        self.client_works_for_valid_json_messages('basic', serialised_payload)


class TestEnhancedKinesisClient(KinesisMixin):
    """Test Enhanced Kinesis Client"""

    def test_enhanced_kinesis_client_handles_valid_json(self, serialised_payload):
        self.client_works_for_valid_json_messages('enhanced', serialised_payload)

