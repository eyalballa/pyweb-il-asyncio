import click as click


from src.data_classes.data_classes_for_integration import IntegrationSettings
from src.sync_classes.data_enricher import DataEnricher
from src.sync_classes.sync_db_connecton import db_connection, DBConnection
from src.sync_classes.sync_integration import Integration
from src.sync_classes.sync_queue import queue_conn
from src.utils import setup_logs_and_monitoring

GET_CUSTOMER_SETTINGS_QUERY = "select query for customer settings {}"


def get_customer_settings(db_conn: DBConnection, customer_id: str):
    setting = db_conn.execute_and_get_first(GET_CUSTOMER_SETTINGS_QUERY.format(customer_id))
    settings_obj = IntegrationSettings.parse_obj(setting)
    return settings_obj


@click.command('run integration read customer from queue')
@click.option('-l', '--log-level', default='info', show_default=True, envvar='LOG_LEVEL', help='log level')
@click.option('-d', '--db-uri', type=str, required=True, help='db connection string')
@click.option('-q', '--queue-uri', type=str, required=True, help='customer service uri')
def main(log_level: str, db_uri: str, queue_uri: str):
    setup_logs_and_monitoring(log_level)
    with queue_conn(queue_uri) as queue:
        with db_connection(db_uri) as db_conn:
            while queue_item := queue.get_item():
                settings = get_customer_settings(db_conn, queue_item.customer_id)  # sync call
                enricher = DataEnricher(db_conn, queue_item.customer_id)
                integration = Integration(settings, enricher)
                integration.run_for_customer()


if __name__ == '__main__':
    main(auto_envvar_prefix='ENRICH')