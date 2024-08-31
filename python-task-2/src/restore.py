import os

from networkx import is_empty
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
            filenames = os.listdir(self.file)
            self.backup_version = max(filenames)

    def restore_sql(self, sql: str) -> str:
        if self.db_type == 'mysql':
            db = mysql.MySQL(connection_string=self.connection_string, database_name=self.database_name)
            return db.restore_database_sql(sql=sql)



    def restore_database(self):
        """
        Delegates restoration tasks to another methods
        """
        version_folder = f'{self.file}/{self.backup_version}'
        files = os.listdir(version_folder)
        sql = ''
        if self.restore_type:
            pass
        else:
            for file in files:
                with open(version_folder + '/' + file, 'r') as f:
                    sql = f.read()
                self.restore_sql(sql=sql)


