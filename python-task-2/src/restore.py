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
        # TODO: Make selection menu for database backups
        # TODO: Add 'SELECT ALL' feature
        version_folder = f'{self.file}/{self.backup_version}'
        files = os.listdir(version_folder)
        if len(files) > 1:
            file_number = 0
            files_list = []
            sql = ''
            for file in files:
                files_list.append(file)
                print(f'[{file_number}] {file}')
                file_number += 1
            isok = False
            choice_list = []
            while not isok:
                choice = input("Please select a file to restore (if you want to restore multiple files, make spaces between numbers): ")
                choice_list = choice.strip().split(' ')
                for number in choice:
                    if int(number) and int(number) < len(files_list) or number == '*':
                        isok = True
                    else:
                        print('Please insert correct number!')
            print(choice_list)
            for number in choice_list:
                sql += files_list[int(number)]
                print(sql)

        else:
            with open(files[0], 'r') as f:
                sql = f.read()
            return self.restore_sql(sql=sql)

