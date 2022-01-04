import asyncio

import httpx

from src.data_classes.data_classes_for_integration import IntegrationSettings, IntegrationItem
from src.async_classes.async_data_enricher import DataEnricher
from src.utils import batch


class Integration:
    def __init__(self, settings: IntegrationSettings, enricher: DataEnricher):
        self.settings = settings
        self.enricher = enricher
        self.max_futures = 9999
        self.integration_batch_size = 1000

    async def get_pages(self, count):
        pages_read = 0
        for i in range(count):
            yield await self.get_page(pages_read)
            pages_read += 1

    async def page_count(self):
        async with httpx.AsyncClient() as client:
            response = await client.get(f'{self.settings.integration_uri}/page_count')
        response.raise_for_status()
        return response.json()

    async def get_page(self, page_number):
        async with httpx.AsyncClient() as client:
            response = await client.get(f'{self.settings.integration_uri}/pages')
        response.raise_for_status()
        return response.json()

    async def run_for_customer_handling_exceptions(self):
        async for futures in self.get_customer_enrich_requests():
            for chunk in batch(futures, self.integration_batch_size):
                results = await asyncio.gather(*chunk, return_exceptions=True)
                await self.handle_errors(results)

    async def run_for_customer(self):
        async for enrich_items in self.get_customer_enrich_requests():
            for chunk in batch(enrich_items, self.integration_batch_size):
                await asyncio.gather(*chunk)

    async def get_customer_enrich_requests(self):
        enrich_items = []
        page_count = self.page_count()
        async for page in self.get_pages(page_count):
            for item in page:
                integration_item = IntegrationItem.parse_obj(item)
                enrich_items.append(self.enricher.enrich_item(integration_item))
                if len(enrich_items) > self.max_futures:
                    yield enrich_items
                    enrich_items = []
        yield enrich_items

    async def handle_errors(self, results):
        # handle errors in the results
        pass
