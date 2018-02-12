"""Interface with the Taxonomy API from local files or from git repository

TODO: Replace files with API once ready (RDSS-1239).
"""

import os
import json

from git import Repo


ACCESS_TYPE = 1
CHECKSUM_TYPE = 2
DATE_TYPE = 3
EDU_PERSON_SCOPED_AFFILIATION = 4
FILE_USE = 5
IDENTIFIER_TYPE = 6
OBJECT_VALUE = 7
ORGANISATION_ROLE = 8
ORGANISATION_TYPE = 9
PERSON_IDENTIFIER_TYPE = 10
PERSON_ROLE = 11
PRESERVATION_EVENT_TYPE = 12
RELATION_TYPE = 13
RESOURCE_TYPE = 14
STORAGE_STATUS = 15
STORAGE_TYPE = 16
UPLOAD_STATUS = 17

VOCAB_FILE_LOOKUP = {
    ACCESS_TYPE: 'accessType.json',
    CHECKSUM_TYPE: 'checksumType.json',
    DATE_TYPE: 'dateType.json',
    EDU_PERSON_SCOPED_AFFILIATION: 'eduPersonScopedAffiliation.json',
    FILE_USE: 'fileUse.json',
    IDENTIFIER_TYPE: 'identifierType.json',
    OBJECT_VALUE: 'objectValue.json',
    ORGANISATION_ROLE: 'organisationRole.json',
    ORGANISATION_TYPE: 'organisationType.json',
    PERSON_IDENTIFIER_TYPE: 'personIdentifierType.json',
    PERSON_ROLE: 'personRole.json',
    PRESERVATION_EVENT_TYPE: 'preservationEventType.json',
    RELATION_TYPE: 'relationType.json',
    RESOURCE_TYPE: 'resourceType.json',
    STORAGE_STATUS: 'storageStatus.json',
    STORAGE_TYPE: 'storageType.json',
    UPLOAD_STATUS: 'uploadStatus.json'
}

TEMP_GIT_REPONAME = 'temp_taxonomydata'


class VocabularyNotFound(Exception):
    pass


class ValueNotFound(Exception):
    pass


class TaxonomyClientBase(object):

    def _get_file_as_dict(self, file_path):
        """Open file path and return as dict."""
        with open(file_path) as f:
            return json.load(f)

    def _get_filedir(self):
        pass

    def _get_vocab_dict(self, vocab_id):
        """Get a vocabulary by ID."""

        base_dir = self._get_filedir()
        file_name = None

        try:
            file_name = VOCAB_FILE_LOOKUP[vocab_id]
        except KeyError:
            raise VocabularyNotFound

        path = os.path.join(base_dir, file_name)
        return self._get_file_as_dict(path)

    def get_by_name(self, vocab_id, name):
        """Get a vocab item by name."""
        values_dict = self._get_vocab_dict(vocab_id)
        values = values_dict.get('vocabularyValues', [])
        for val in values:
            val_name = val['valueName']
            if val_name == name:
                return val['valueId']

        raise ValueNotFound


class TaxonomyGitClient(TaxonomyClientBase):
    """Taxonomy API Client for use with files
     checked into a public git repository"""

    def __init__(self, repo_url):
        self.repo_url = repo_url
        self.temp_reponame = TEMP_GIT_REPONAME
        self._initclient()

    def _initclient(self):
        temp_dir = self._get_filedir()
        Repo.clone_from(self.repo_url, temp_dir)

    def _get_filedir(self):
        current_dir = os.path.abspath(os.getcwd())
        temp_dir = os.path.join(
            current_dir, self.temp_reponame, 'datamodels'
        )
        return temp_dir


class TaxonomyLocalClient(TaxonomyClientBase):
    """Taxonomy API Client for use with local taxonomy files."""

    def _get_filedir(self):
        current_dir = os.path.dirname(os.path.abspath(os.getcwd()))
        base_dir = os.path.join(current_dir, 'taxonomy_data/')
        return base_dir
