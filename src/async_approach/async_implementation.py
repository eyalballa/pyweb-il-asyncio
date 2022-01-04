import asyncio

import click as click
from aiodebug import log_slow_callbacks

from src.data_classes.data_classes_for_integration import IntegrationSettings
from src.async_classes.async_data_enricher import DataEnricher
from src.async_classes.async_db_connection import db_connection, DBConnection
from src.async_classes.async_integration import Integration
from src.async_classes.async_queue import queue_conn
from src.utils import setup_logs_and_monitoring

GET_CUSTOMER_SETTINGS_QUERY = "select query for customer settings {}"


async def get_customer_settings(db_conn: DBConnection, customer_id: str):
    setting = await db_conn.execute_and_get_first(GET_CUSTOMER_SETTINGS_QUERY.format(customer_id))
    settings_obj = IntegrationSettings.parse_obj(setting)
    return settings_obj


@click.command('asyncio implementation of integration')
@click.option('-l', '--log-level', default='info', show_default=True, envvar='LOG_LEVEL', help='log level')
@click.option('-d', '--db-uri', type=str, required=True, help='db connection string')
@click.option('-q', '--queue-uri', type=str, required=True, help='customer service uri')
def main(log_level: str, db_uri: str, queue_uri: str):
    log_slow_callbacks.enable(0.5)
    asyncio.run(async_main(log_level, db_uri, queue_uri))


async def async_main(log_level: str, db_uri: str, queue_uri: str):
    setup_logs_and_monitoring(log_level)
    async with queue_conn(queue_uri) as queue:
        async with db_connection(db_uri) as db_conn:
            while queue_item := await queue.get_item():
                settings = await get_customer_settings(db_conn, queue_item.customer_id)  # sync call
                enricher = DataEnricher(db_conn, queue_item.customer_id)
                integration = Integration(settings, enricher)
                await integration.run_for_customer()


def handle_exception(loop, context):
    # handle exceptions for loop with context
    pass


async def async_main_with_exception_handler(log_level: str, db_uri: str, queue_uri: str):
    loop = asyncio.get_event_loop()
    loop.set_exception_handler(handle_exception)
    await async_main(log_level, db_uri, queue_uri)


async def monitor_tasks():
        tasks = [
            t for t in asyncio.all_tasks()
            if t is not asyncio.current_task()
        ]
        [t.print_stack(limit=5) for t in tasks]


if __name__ == '__main__':
    main(auto_envvar_prefix='ENRICH')