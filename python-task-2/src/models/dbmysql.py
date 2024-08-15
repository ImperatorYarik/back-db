import mysql.connector

from src.models.database import Database


class MySQL(Database):
    def __init__(self, database_name: str, connection_string: str, table_name: str = None) -> None:
        self.database_name = database_name
        self.connection_string = connection_string
        self.table_name = table_name
        self.connection = mysql.connector.connect(option_files=self.connection_string)

    def get_database_structure(self) -> str:
        cursor = self.connection.cursor()
        table = []
        cursor.execute('SHOW TABLES')
        tables = [row[0] for row in cursor.fetchall()]

        structure = ''
        for table in tables:
            cursor.execute(f'SHOW CREATE TABLE {table}')
            create_table = cursor.fetchall()[1]

            structure += create_table + '\n\n'

        return structure

    def get_database_data(self) -> dict:
        pass

    def get_table(self) -> dict:
        pass

    def restore_database_structure(self, database_data) -> bool:
        pass

    def restore_database_data(self, database_data) -> bool:
        pass

    def restore_table(self, table_data) -> bool:
        pass
