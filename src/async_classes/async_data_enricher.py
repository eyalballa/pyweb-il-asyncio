from src.data_classes.data_classes_for_integration import IntegrationItem


class DataEnricher:
    def __init__(self, db_conn: str, customer_id: str):
        self.db_conn = db_conn
        self.customer_id = customer_id

    async def enrich_item(self, item: IntegrationItem):
        ## enrich an item in the db with data read from the integration item
        pass