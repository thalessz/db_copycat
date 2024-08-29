from sqlalchemy.testing.plugin.plugin_base import logging

from copycat import *
import os
from dotenv import load_dotenv

load_dotenv()
def main():
    source_config = {
        'DB_HOST': os.getenv('FDB_HOST'),
        'DB_USER': os.getenv('FDB_USER'),
        'DB_PASSWORD': os.getenv('FDB_PASSWORD'),
        'DB_DATABASE': os.getenv('FDB_DATABASE'),
        'DB_TYPE': 'fdb'
    }

    target_config = {
        'DB_HOST': os.getenv('SQL_HOST'),
        'DB_USER': os.getenv('SQL_USER'),
        'DB_PASSWORD': os.getenv('SQL_PASSWORD'),
        'DB_DATABASE': os.getenv('SQL_DATABASE'),
        'DB_TYPE': 'mysql'
    }

    migrationManager = MigrationManager(source_config, target_config)
    try:
        migrationManager.source_connector.connect()
        migrationManager.target_connector.connect()
        table_name = 'FUNCIONARIO'
        migrationManager.clone_database(table_name)
        print(f'Table {table_name.lower()} succesfully cloned!')
    except Exception as e:
        print(f'ERROR: {e}')
    finally:
        migrationManager.source_connector.close_connection()
        migrationManager.target_connector.close_connection()

if __name__ == "__main__":
    main()

