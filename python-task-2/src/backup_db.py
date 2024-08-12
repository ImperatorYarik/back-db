import os
from datetime import datetime

from src.db import Database


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

    def backup_structure(self):
        if self.is_structure:
            schema = super().get_database_schema()
            current_time = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
            save_path = f'{self.filepath}/{current_time}'
            os.makedirs(save_path, exist_ok=True)
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
            with open(f'{save_path}/{current_time}-structure' + '.sql', 'w') as file:
                file.write(result)
