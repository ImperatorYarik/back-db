import os

from src.models import dbmysql as mysql


class Restore:
    def __init__(self, database_name: str, connection_string: str, db_type: str, file: str = None,
                 backup_version: str = None, restore_type: str = None,
                 table_name: str = None) -> None:
        self.db_type = db_type
        self.database_name = database_name
        self.file = file
        if self.file is None:
            self.file = f'backup/{database_name}-{db_type}'
        self.backup_version = backup_version
        self.restore_type = restore_type
        self.table_name = table_name
        self.connection_string = connection_string
        self.backup_version = backup_version
        if self.backup_version is None:
            all_versions = []
            filenames = os.listdir(self.file)
            for filename in filenames:
                temp = filename.split('-')
                print(temp)
                if temp[1] == f'{self.restore_type}.sql':
                    all_versions.append(temp[0])
                    self.backup_version = max(all_versions)

    def restore_structure(self, database_structure):
        if self.db_type == 'mysql':
            db = mysql.MySQL(connection_string=self.connection_string, database_name=self.database_name)
            return db.restore_database_structure(database_structure=database_structure)

    def restore_data(self, database_data):
        if self.db_type == 'mysql':
            db = mysql.MySQL(connection_string=self.connection_string, database_name=self.database_name)
            return db.restore_database_data(database_data=database_data)

    def restore_database(self):
        """
        Delegates restoration tasks to another methods
        """
        match self.restore_type:
            case 'structure':
                with open(f'{self.file}/{self.backup_version}-structure.sql', 'r') as file:
                    structure = file.read()
                return self.restore_structure(structure)

            case 'data':
                with open(f'{self.file}/{self.backup_version}-data.sql', 'r') as file:
                    data = file.read()
                #print(data)
                return self.restore_data(data)
            case _:
                with open(f'{self.file}/{self.backup_version}-full.sql', 'w') as file:
                    data = file.read()

                elements = data.split('--DATA--')
                return self.restore_structure(elements[0]), self.restore_data(elements[1])

