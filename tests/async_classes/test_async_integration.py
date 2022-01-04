from unittest.mock import patch

import pytest
from pytest_httpx import HTTPXMock

from src.async_classes.async_data_enricher import DataEnricher
from src.async_classes.async_integration import Integration
from src.data_classes.data_classes_for_integration import IntegrationSettings


@pytest.fixture
def integration_uri():
    return 'uri'


@pytest.fixture
def db_conn():
    return 'db_connection'


@pytest.fixture
def integration_uri():
    return 'uri'


@pytest.fixture
def customer_id():
    return 'customer'


@pytest.fixture
def integration_settings(integration_uri):
    return IntegrationSettings(integration_uri=integration_uri)


@pytest.fixture
def enricher(db_conn, customer_id):
    return DataEnricher(db_conn, customer_id)


@pytest.fixture
def async_integration(integration_settings, enricher):
    return Integration(integration_settings, enricher)


@pytest.mark.asyncio
async def test_page_count_returns_json(async_integration, httpx_mock: HTTPXMock, integration_uri):
    response = {
        "count": 10
    }
    httpx_mock.add_response(url=f'{integration_uri}/page_count', data=response)
    result = await async_integration.page_count()
    assert result == response


@pytest.mark.asyncio
async def test_page_count_exception_on_error_status(async_integration, httpx_mock: HTTPXMock, integration_uri):
    response = {
        "count": 10
    }
    httpx_mock.add_response(url=f'{integration_uri}/page_count', status_code=400)
    with pytest.raises(Exception):
        result = await async_integration.page_count()


@pytest.mark.asyncio
@patch("src.async_classes.async_integration.Integration.get_page", return_value="page")
async def test_get_pages_call_get_page_according_to_count(mock_get_page, async_integration):
    pages = []
    async for page in async_integration.get_pages(5):
        pages.append(page)

    assert len(pages) == 5
    assert mock_get_page.await_count == 5
