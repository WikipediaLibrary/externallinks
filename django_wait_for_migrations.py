#!/usr/bin/env python
"""Wrapper for Django's command-line utility for administrative tasks."""
import os
import sys
import logging
from time import sleep

try:
    from django.db.migrations.executor import MigrationExecutor
    from django.db.utils import ConnectionHandler, DEFAULT_DB_ALIAS
except ImportError as exc:
    raise ImportError(
        "Couldn't import Django. Are you sure it's installed and "
        "available on your PYTHONPATH environment variable? Did you "
        "forget to activate a virtual environment?"
    ) from exc


def db_migrated(database):
    connections = ConnectionHandler()
    connection = connections[database]
    connection.prepare_database()
    executor = MigrationExecutor(connection)
    targets = executor.loader.graph.leaf_nodes()
    return not executor.migration_plan(targets)


def wait_for_migrations(args):
    try:
        from django import setup
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        raise ImportError(
            "Couldn't import Django. Are you sure it's installed and "
            "available on your PYTHONPATH environment variable? Did you "
            "forget to activate a virtual environment?"
        ) from exc
    setup()
    logger = logging.getLogger(__name__)
    wait = 0
    # Unapplied migrations found.
    while not db_migrated(DEFAULT_DB_ALIAS):
        logger.info("Unapplied migrations found.")
        sleep(1)
        wait += 1

        if wait > 120:
            raise Exception("Migration timeout")

    # All migrations have been applied.
    if db_migrated(DEFAULT_DB_ALIAS):
        logger.info("All migrations have been applied.")
        execute_from_command_line(args)
    else:
        raise Exception("Unknown error.")


if __name__ == "__main__":
    wait_for_migrations(sys.argv)
