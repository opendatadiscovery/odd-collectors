import pytest
from oddrn_generator import CKANGenerator

from odd_collector.domain.plugin import CKANPlugin
from odd_collector.adapters.ckan.mappers.models import (
    Group,
    Organization,
    Dataset,
    Resource,
)


@pytest.fixture
def ckan_adapter_config(request):
    def _create_config(endpoint="/ckan-endpoint"):
        return CKANPlugin(
            type="ckan",
            name="ckan_adapter",
            host="ckan_test.com",
            ckan_endpoint=endpoint,
            port="",
            token=None,
        )

    return _create_config


@pytest.fixture
def ckan_generator(ckan_adapter_config):
    return CKANGenerator(host_settings=ckan_adapter_config().name)


@pytest.fixture
def valid_pagination_request_result(request):
    def _create_request(
        count_number: int = 15, rows_number: int = 5, success: bool = True
    ):
        results = [{"author": f"test{num}"} for num in range(rows_number)]
        return {
            "help": "https://test.com/api/3/action/help_show?name=package_search",
            "success": success,
            "result": {
                "count": count_number,
                "facets": {},
                "results": results,
                "sort": "score desc, metadata_modified desc",
                "search_facets": {},
            },
        }

    return _create_request


@pytest.fixture
def models_group(request):
    def _create_group(name=None):
        return Group(
            data={
                "approval_status": "approved",
                "created": "2017-12-04T18:31:16.524413",
                "description": "",
                "display_name": "test group name",
                "id": name,
                "image_display_url": "",
                "image_url": "",
                "is_organization": False,
                "name": name,
                "num_followers": 0,
                "package_count": 20323,
                "state": "active",
                "title": "test group title",
                "type": "group",
                "packages": [
                    {
                        "author": "test group author",
                        "author_email": "test@gmail.com",
                        "creator_user_id": "f512f73d-bed2-439d-8f6f-3fbc6048b24e",
                        "id": "094ab094-5fb7-4c02-a224-5ae87e1dc7de",
                        "isopen": False,
                        "license_id": "",
                        "license_title": "",
                        "maintainer": "test group author 2",
                        "maintainer_email": "test2@gmail.com",
                        "metadata_created": "2023-01-06T06:15:06.831052",
                        "metadata_modified": "2023-11-29T09:25:26.130214",
                        "name": "test package name",
                        "notes": "test notes",
                        "num_resources": 24,
                        "num_tags": 16,
                        "organization": {
                            "id": "35401933-7aaf-4931-a37d-e226c742db76",
                            "name": "test organization name",
                            "title": "test organization title",
                            "type": "organization",
                            "description": "test organization description",
                            "image_url": "",
                            "created": "2016-01-11T09:32:14.084084",
                            "is_organization": True,
                            "approval_status": "approved",
                            "state": "active",
                        },
                        "owner_org": "35401933-7aaf-4931-a37d-e226c742db76",
                        "private": False,
                        "relationships_as_object": [],
                        "relationships_as_subject": [],
                        "state": "active",
                        "title": "test title",
                        "type": "dataset",
                        "url": None,
                        "version": None,
                    },
                ],
                "extras": [],
                "tags": [],
                "groups": [],
            }
        )

    return _create_group


@pytest.fixture
def models_organization(request):
    def _create_organization(name=None):
        return Organization(
            data={
                "approval_status": "approved",
                "created": "2020-06-11T10:43:29.894113",
                "description": "",
                "display_name": "test organization name",
                "id": "c6f6f6ba-93ab-40ed-8dcf-62d1b678260f",
                "image_display_url": "",
                "image_url": "",
                "is_organization": True,
                "name": name,
                "num_followers": 0,
                "package_count": 7,
                "state": "active",
                "title": "test organization title",
                "type": "organization",
                "extras": [
                    {
                        "group_id": "c6f6f6ba-93ab-40ed-8dcf-62d1b678260f",
                        "id": "a479c82c-b3bd-48ed-9cd6-4c4a11e39571",
                        "key": "contributorID",
                        "state": "active",
                        "value": '[""]',
                    }
                ],
                "tags": [],
                "groups": [],
            }
        )

    return _create_organization


@pytest.fixture
def models_dataset(request):
    def _create_dataset(name=None):
        return Dataset(
            data={
                "author": "test dataset author",
                "author_email": "",
                "creator_user_id": "36924f3c-ba7b-42ed-94e5-fa8ceca6e7f9",
                "id": "802e3d4b-a4ef-4fa3-bff0-990decbd4924",
                "isopen": False,
                "license_id": None,
                "license_title": None,
                "maintainer": "test dataset maintaner",
                "maintainer_email": "",
                "metadata_created": "2020-07-03T12:20:54.148020",
                "metadata_modified": "2021-03-26T16:21:01.761275",
                "name": name,
                "notes": "",
                "num_resources": 1,
                "num_tags": 9,
                "organization": {
                    "id": "c6f6f6ba-93ab-40ed-8dcf-62d1b678260f",
                    "name": "test organization name",
                    "title": "test organization title",
                    "type": "organization",
                    "description": "",
                    "image_url": "",
                    "created": "2020-06-11T10:43:29.894113",
                    "is_organization": True,
                    "approval_status": "approved",
                    "state": "active",
                },
                "owner_org": "c6f6f6ba-93ab-40ed-8dcf-62d1b678260f",
                "private": False,
                "state": "active",
                "title": "test dataset title",
                "type": "dataset",
                "url": "",
                "version": None,
                "extras": [
                    {"key": "author_addressee", "value": ""},
                    {"key": "author_city", "value": ""},
                    {"key": "author_country", "value": ""},
                    {"key": "author_street", "value": ""},
                    {
                        "key": "author_url",
                        "value": "",
                    },
                    {"key": "author_zip", "value": ""},
                ],
                "groups": [
                    {
                        "description": "",
                        "display_name": "test group name",
                        "id": "soci",
                        "image_display_url": "",
                        "name": "soci",
                        "title": "test group title",
                    },
                ],
                "resources": [
                    {
                        "cache_last_updated": None,
                        "cache_url": None,
                        "created": "2020-10-08T13:42:55.987609",
                        "description": "",
                        "format": "XML",
                        "hash": "",
                        "id": "408d4834-6075-4952-8772-8d2ff0afa118",
                        "language": '["deutsch/englisch"]',
                        "last_modified": None,
                        "license": "",
                        "licenseAttributionByText": "",
                        "metadata_modified": "2020-10-08T13:42:55.987609",
                        "mimetype": None,
                        "mimetype_inner": None,
                        "name": "test resource name",
                        "package_id": "802e3d4b-a4ef-4fa3-bff0-990decbd4924",
                        "position": 0,
                        "resource_type": None,
                        "size": 0,
                        "state": "active",
                        "url": "",
                        "url_type": None,
                    }
                ],
                "tags": [
                    {
                        "display_name": "test tag name",
                        "id": "bfc628e8-9dd5-4fd7-819d-d9c18dc4a435",
                        "name": "test tag name",
                        "state": "active",
                        "vocabulary_id": None,
                    },
                ],
                "relationships_as_subject": [],
                "relationships_as_object": [],
            }
        )

    return _create_dataset


@pytest.fixture
def models_resource(request):
    def _create_resource(name=None):
        return Resource(
            data={
                "cache_last_updated": None,
                "cache_url": None,
                "created": "2020-10-08T13:42:55.987609",
                "description": "Test description",
                "format": "XML",
                "hash": "",
                "id": "408d4834-6075-4952-8772-8d2ff0afa118",
                "language": '["deutsch/englisch"]',
                "last_modified": None,
                "license": "http://test_licenses/license",
                "licenseAttributionByText": "",
                "metadata_modified": "2020-10-08T13:42:55.987609",
                "mimetype": None,
                "mimetype_inner": None,
                "name": name,
                "package_id": "802e3d4b-a4ef-4fa3-bff0-990decbd4924",
                "position": 0,
                "resource_type": None,
                "size": 0,
                "state": "active",
                "url": "https://www.test_blobs/test_name.xml",
                "url_type": None,
            }
        )

    return _create_resource
