import requests

from src.data_classes.data_classes_for_integration import IntegrationSettings, IntegrationItem
from src.sync_classes.data_enricher import DataEnricher


class Integration:
    def __init__(self, settings: IntegrationSettings, enricher: DataEnricher):
        self.settings = settings
        self.enricher = enricher

    def get_pages(self):
        count = self.page_count()
        pages_read = 0
        while pages_read < count:
            yield self.get_page(pages_read)
            pages_read += 1

    def page_count(self):
        response = requests.get(f'{self.settings.integration_uri}/page_count')
        response.raise_for_status()
        return response.json()

    def get_page(self, page_number):
        response = requests.get(f'{self.settings.integration_uri}/pages')
        response.raise_for_status()
        return response.json()

    def run_for_customer(self):
        for page in self.get_pages():
            for item in page:
                integration_item = IntegrationItem.parse_obj(item)
                self.enricher.enrich_item(integration_item)
