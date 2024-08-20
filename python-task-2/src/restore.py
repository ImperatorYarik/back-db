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

    def restore_structure(self, database_structure):
        if self.db_type == 'mysql':
            db = mysql.MySQL(connection_string=self.connection_string, database_name=self.database_name)
            return db.restore_database_structure(database_structure=database_structure)

    def restore_database(self):
        """
        Delegates restoration tasks to another methods
        """
        match self.restore_type:
            case 'structure':
                if self.backup_version is None:
                    all_versions = []
                    filenames = os.listdir(self.file)
                    for filename in filenames:
                        all_versions.append(filename.split('-')[0])
                    self.backup_version = max(all_versions)
                with open(f'{self.file}/{self.backup_version}-structure.sql', 'r') as file:
                    structure = file.read()
                return self.restore_structure(structure)