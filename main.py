import logging
from dotenv import load_dotenv
from copycat import MigrationManager
import os

load_dotenv()

def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

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
        columns = ['CODIGO', 'NOMECOMPLETO', 'LOCAL', 'FUNCAO', 'SENHA']

        logging.info(f'Criando a tabela {table_name.lower()} no banco de dados de destino...')
        migrationManager.create_table(table_name, columns)
        logging.info(f'Tabela {table_name.lower()} criada com sucesso!')

        logging.info(f'Inserindo dados na tabela {table_name.lower()}...')
        migrationManager.insert_data(table_name, columns)
        logging.info(f'Dados inseridos na tabela {table_name.lower()} com sucesso!')

    except Exception as e:
        logging.error(f'Erro durante a clonagem da tabela {table_name.lower()}: {e}')
    finally:
        migrationManager.source_connector.close_connection()
        migrationManager.target_connector.close_connection()

if __name__ == "__main__":
    main()
