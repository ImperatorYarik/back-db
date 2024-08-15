
import mysql.connector

from src.models.database import Database


class MySQL(Database):
    def __init__(self, database_name: str, connection_string: str, table_name: str = None) -> None:
        self.database_name = database_name
        self.connection_string = connection_string
        self.table_name = table_name
        connection_params = self.parse_connection_string(connection_string)

        self.connection = mysql.connector.connect(
            user=connection_params['user'],
            password=connection_params['password'],
            host=connection_params['host'],
            port=connection_params['port'],
            database=database_name
        )

    def get_database_structure(self) -> str:
        cursor = self.connection.cursor()
        cursor.execute('SHOW TABLES')
        tables = [row[0] for row in cursor.fetchall()]

        structure = f'CREATE SCHEMA {self.database_name};\nUSE {self.database_name}\n\n'
        for table in tables:
            cursor.execute(f'SHOW CREATE TABLE {table}')
            create_table = cursor.fetchall()

            structure += create_table[0][1] + '\n\n'
            # print(structure)
        return structure

    def get_database_data(self) -> str:
        pass

    def get_table(self) -> dict:
        pass

    def restore_database_structure(self, database_data) -> bool:
        pass

    def restore_database_data(self, database_data) -> bool:
        pass

    def restore_table(self, table_data) -> bool:
        pass

    def parse_connection_string(self, connection_string: str) -> dict:
        import re
        pattern = r'mysql://(?P<user>[^:]+):(?P<password>[^@]+)@(?P<host>[^:]+):(?P<port>\d+)/'
        match = re.match(pattern, connection_string)
        if not match:
            raise ValueError("Invalid connection string format")
        return match.groupdict()
