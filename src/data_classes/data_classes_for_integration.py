from pydantic import BaseModel


class IntegrationSettings(BaseModel):
    # contains the settings for integration
    integration_uri: str


class IntegrationItem(BaseModel):
    item_id: str


class QueueItem(BaseModel):
    customer_id: str
