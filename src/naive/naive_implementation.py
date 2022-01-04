from typing import Any

import click as click
import requests

from src.data_classes.data_classes_for_integration import IntegrationSettings
from src.sync_classes.data_enricher import DataEnricher
from src.sync_classes.sync_db_connecton import db_connection, DBConnection
from src.sync_classes.sync_integration import Integration
from src.utils import setup_logs_and_monitoring

GET_CUSTOMER_SETTINGS_QUERY = "select query for customer settings {}"


def get_customer_list(customer_service_uri: str) -> dict[Any]:
    """
    gets the customer list and their properties
    :param customer_service_uri: uri for customer service
    :return:
    returns dict with customers and their properties in the form:
    {customer_id: {<customer_details>, customer_id: {<customer_details>}...}

    """
    response = requests.get(customer_service_uri)
    response.raise_for_status()

    return response.json()


def get_customer_settings(db_conn: DBConnection, customer_id: str):
    setting = db_conn.execute_and_get_first(GET_CUSTOMER_SETTINGS_QUERY.format(customer_id))
    settings_obj = IntegrationSettings.parse_obj(setting)
    return settings_obj


@click.command('run naive integration flow')
@click.option('-l', '--log-level', default='info', show_default=True, envvar='LOG_LEVEL', help='log level')
@click.option('-d', '--db-uri', type=str, required=True, help='db connection string')
@click.option('-d', '--customer-service-uri', type=str, required=True, help='customer service uri')
def main(log_level: str, db_uri: str, customer_service_uri: str):
    setup_logs_and_monitoring(log_level)
    customers = get_customer_list(customer_service_uri)
    with db_connection(db_uri) as db_conn:
        for customer_id in customers.keys():
            settings = get_customer_settings(db_conn, customer_id)  # sync call
            enricher = DataEnricher(db_conn, customer_id)
            integration = Integration(settings, enricher)
            integration.run_for_customer()


if __name__ == '__main__':
    main(auto_envvar_prefix='ENRICH')