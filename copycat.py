from sqlalchemy import create_engine

class DatabaseConnector:
    def __init__(self, config):
        self.host = config['host']
        self.user = config['user']
        self.password = config['password']
        self.database = config['database']
        self.db_type = config['db_type']
        self.engine = None

    def connect(self):
        try:
            if self.db_type == 'mysql':
                url = f'mysql+mysqlconnector://{self.user}:{self.password}@{self.host}/{self.database}'
            elif self.db_type == 'fdb':
                url = f'firebird+fdb://{self.user}:{self.password}@{self.host}/{self.database}'
            else:
                raise TypeError("Database not found. Try again with the specified types in github.com/thalessz/db_copycat.git")
            self.engine = create_engine(url)
        except Exception as e:
            raise Exception(f"ERROR: {e}")

    def close_connection(self):
        if self.engine:
            self.engine.dispose()
            print("Connection Closed")

class MigrationManager:
    def __init__(self, source_config, target_config):
        self.source_connector = DatabaseConnector(source_config)
        self.target_connector = DatabaseConnector(target_config)

    def clone_database(self, table_name, columns=None):
        with self.source_connector.engine.connect() as source_conn, self.target_connector.engine.connect() as target_conn:
            if columns:
                source_query = f"SELECT {', '.join(columns)} FROM {table_name}"
            else:
                source_query = f'SELECT * FROM {table_name}'

            table_structure = self.get_table_structure(table_name)
            self.create_table(table_name, table_structure, target_conn)

            result = source_conn.execute(source_query)
            for row in result:
                insert_query = f'INSERT INTO {table_name} VALUES ({", ".join(["%s"] * len(row))})'
                target_conn.execute(insert_query, row)

    def get_table_structure(self, table_name):
        with self.source_connector.engine.connect() as conn:
            structure = f'''
            SELECT RDB$FIELD_NAME, RDB$TYPE, RDB$NULL_FLAG
            FROM RDB$RELATION_FIELDS
            WHERE RDB$RELATION_NAME = '{table_name}'
            '''
            stResult = conn.execute(structure)
            return stResult.fetchall()

    def create_table(self, table_name, structure, conn):
        columns = []
        for field_name, field_type, null_flag in structure:
            sql_type = {
                7: 'INTEGER',
                8: 'FLOAT',
                9: 'DECIMAL',
                10: 'VARCHAR',
                11: 'CHAR',
                12: 'DATE',
                13: 'TIME',
                14: 'TIMESTAMP'
            }.get(field_type, 'UNKNOWN')

            null_constraint = 'NOT NULL' if null_flag == 0 else ''
            columns.append(f'{field_name} {sql_type} {null_constraint}')

        ct_command = f'CREATE TABLE {table_name} ({", ".join(columns)})'
        conn.execute(ct_command)
