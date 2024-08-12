import os
from datetime import datetime

from src.db import Database
from sqlalchemy import text

class BackupDatabase(Database):
    def __init__(self, database_url: str, database_name: str, is_structure: bool = False, is_data: bool = False,
                 is_full: bool = False,
                 is_tables: bool = False,
                 tables: list = None,
                 filepath: str = None):
        super().__init__(database_url=database_url, database_name=database_name)
        self.is_structure = is_structure
        self.is_data = is_data
        self.is_full = is_full
        self.is_tables = is_tables
        self.tables = tables
        if filepath is None:
            self.filepath = f'backups/{self.database_name}'
        else:
            self.filepath = filepath

    def backup(self):
        current_time = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        save_path = f'{self.filepath}/{current_time}'
        os.makedirs(save_path, exist_ok=True)

        if self.is_structure:
            backup = self.backup_structure()
            with open(f'{save_path}/structure.sql', 'w') as f:
                f.write(backup)
        if self.is_data:
            backup = self.backup_data()
            with open(f'{save_path}/data.sql', 'w') as f:
                f.write(backup)
        if self.is_full:
            backup = f'{self.backup_structure()}\n\n{self.backup_data()}'
            with open(f'{save_path}/full.sql', 'w') as f:
                f.write(backup)

    def backup_structure(self):
        schema = super().get_database_schema()
        result = f'CREATE SCHEMA {self.database_name};\nUSE {self.database_name};\n\n'
        column_definitions = []
        result += ''
        for table_name, columns in schema.items():
            for column in columns:
                column_definition = f'{column["name"]} {column["type"]}'
                if not column["nullable"]:
                    column_definition += ' NOT NULL'
                if column["primary_key"]:
                    column_definition += ' PRIMARY KEY'
                if column["default"]:
                    column_definition += f' DEFAULT {column["default"]}'
                column_definitions.append(column_definition)

            column_str = ', '.join(column_definitions)
            result += f'CREATE TABLE {table_name} ({column_str});\n'
        return result

    def backup_data(self):
        data = self.get_all_data()
        result = f'USE {self.database_name};\n\n'
        values_str = ''
        for table_name, rows in data.items():
            for row in rows:
                values_str += ', '.join(f'{v}' for v in row.values())
                values_str += '),\n '


            result += f'INSERT INTO {table_name} VALUES ({values_str});\n'
        return result
