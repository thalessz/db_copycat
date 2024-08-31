import pandas as pd

class DatabaseConnector:
    def __init__(self, config):
        self.host = config['DB_HOST']
        self.user = config['DB_USER']
        self.password = config['DB_PASSWORD']
        self.database = config['DB_DATABASE']
        self.db_type = config['DB_TYPE']
        self.engine = None

    def connect(self):
        try:
            if self.db_type == 'mysql':
                import mysql.connector
                self.engine = mysql.connector.connect(
                    host=self.host,
                    user=self.user,
                    password=self.password,
                    database=self.database
                )
            elif self.db_type == 'fdb':
                import fdb
                self.engine = fdb.connect(
                    host=self.host,
                    user=self.user,
                    password=self.password,
                    database=self.database
                )
            else:
                raise TypeError("Database not found. Try again with the specified types in github.com/thalessz/db_copycat.git")
        except Exception as e:
            raise Exception(f"ERROR: {e}")

    def close_connection(self):
        if self.engine:
            self.engine.close()
            print("Connection Closed")

class MigrationManager:
    def __init__(self, source_config, target_config):
        self.source_connector = DatabaseConnector(source_config)
        self.target_connector = DatabaseConnector(target_config)

    def create_table(self, table_name, columns=None):
        self.source_connector.connect()
        self.target_connector.connect()

        try:
            # Obter a estrutura da tabela
            table_structure = self.get_table_structure(table_name)
            self.create_table_in_target(table_name, table_structure, self.target_connector.engine)
        finally:
            self.source_connector.close_connection()
            self.target_connector.close_connection()

    def insert_data(self, table_name, columns=None):
        self.source_connector.connect()
        self.target_connector.connect()

        try:
            # Se colunas foram especificadas, use-as na consulta
            if columns:
                source_query = f"SELECT {', '.join(columns)} FROM {table_name}"
            else:
                source_query = f'SELECT * FROM {table_name}'

            # Carregar dados em um DataFrame
            df = pd.read_sql(source_query, self.source_connector.engine)

            # Tratar dados de data/hora
            if 'LOCAL' in df.columns:
                df['LOCAL'] = pd.to_datetime(df['LOCAL'], errors='coerce')  # Converte e trata erros
                df = df.dropna(subset=['LOCAL'])  # Remove linhas com valores inválidos

            # Tratar valores ausentes
            df.fillna({'COLUMN_NAME': 'default_value'}, inplace=True)  # Substitua 'COLUMN_NAME' e 'default_value' conforme necessário

            # Remover duplicatas
            df.drop_duplicates(inplace=True)

            # Inserir os dados tratados no banco de dados de destino
            cursor = self.target_connector.engine.cursor()
            for index, row in df.iterrows():
                insert_query = f'INSERT INTO {table_name} ({", ".join(df.columns)}) VALUES ({", ".join(["%s"] * len(row))})'
                cursor.execute(insert_query, tuple(row))

            cursor.close()
        finally:
            self.source_connector.close_connection()
            self.target_connector.close_connection()
    @staticmethod
    def create_table_in_target(self, table_name, structure, conn):
        cursor = conn.cursor()
        columns = []
        for field_name, field_type, null_flag in structure:
            sql_type = {
                7: 'INT',          # INTEGER
                8: 'FLOAT',        # FLOAT
                9: 'DECIMAL',      # DECIMAL
                10: 'VARCHAR(255)', # VARCHAR
                11: 'CHAR(255)',   # CHAR
                12: 'DATE',        # DATE
                13: 'TIME',        # TIME
                14: 'DATETIME'     # TIMESTAMP
            }.get(field_type, 'TEXT')  # Usar TEXT como padrão se o tipo for desconhecido

            null_constraint = 'NOT NULL' if null_flag == 0 else ''
            columns.append(f'{field_name} {sql_type} {null_constraint}'.strip())

        ct_command = f'CREATE TABLE {table_name} ({", ".join(columns)})'
        print(f"Executing SQL: {ct_command}")  # Para depuração
        cursor.execute(ct_command)  # Use o cursor para executar a consulta
        cursor.close()  # Feche o cursor após a execução

    def get_table_structure(self, table_name):
        conn = self.source_connector.engine.cursor()
        try:
            structure = f"""
            SELECT 
                RDB$RELATION_FIELDS.RDB$FIELD_NAME AS field_name,
                RDB$FIELDS.RDB$FIELD_TYPE AS field_type,
                RDB$RELATION_FIELDS.RDB$NULL_FLAG AS null_flag
            FROM 
                RDB$RELATION_FIELDS
                    JOIN RDB$FIELDS ON RDB$RELATION_FIELDS.RDB$FIELD_SOURCE = RDB$FIELDS.RDB$FIELD_NAME  
                    WHERE 
                        RDB$RELATION_FIELDS.RDB$RELATION_NAME = ?
            """
            conn.execute(structure, (table_name,))
            return conn.fetchall()
        finally:
            conn.close()
