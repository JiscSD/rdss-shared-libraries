import json
import os
import pytest
import shutil

from rdsslib.taxonomy.taxonomy_client import TaxonomyGitClient
from rdsslib.taxonomy.taxonomy_client import CHECKSUM_TYPE, TEMP_GIT_REPONAME


class TestTaxonomyClients(object):

    temp_path = 'temp'

    @pytest.fixture
    def client(self, monkeypatch):
        monkeypatch.setattr(TaxonomyGitClient, '_initclient', self.mock_clone)
        client = TaxonomyGitClient('test_url', 'v0.1.0', self.temp_path)
        yield client
        shutil.rmtree(os.path.join(self.temp_path))

    def mock_clone(self, tag):
        json_content = {
            'vocabularyId': 2,
            'vocabularyName': 'checksumType',
            'vocabularyValues': [
                {
                    'valueId': 1,
                    'valueName': 'md5'
                },
                {
                    'valueId': 2,
                    'valueName': 'sha256'
                }
            ]
        }

        os.makedirs(os.path.join(self.temp_path,
                                 TEMP_GIT_REPONAME, 'datamodels'))
        with open(os.path.join(self.temp_path, TEMP_GIT_REPONAME,
                               'datamodels',
                               'checksumType.json'), 'w') as handle:
            handle.write(json.dumps(json_content))

    def test_gitclient_returns_correct_value(self, client):
        value = client.get_by_name(CHECKSUM_TYPE, 'sha256')
        assert value == 2
