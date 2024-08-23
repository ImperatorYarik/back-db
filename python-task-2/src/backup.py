import os
from datetime import datetime
import logging

from src.models import dbmysql as mysql


class Backup:

    def __init__(self, db_type: str, database_name: str, connection_string: str, table_name: str = None,
                 op_type: str = None,
                 is_save_one: bool = True, is_save_multiple: bool = False, save_into: str = None) -> None:
        logger = logging.getLogger(__name__)
        self.db_type = db_type
        self.database_name = database_name
        self.connection_string = connection_string
        self.table_name = table_name
        self.op_type = op_type
        self.is_save_one = is_save_one
        self.is_save_multiple = is_save_multiple
        self.save_into = save_into
        if self.save_into is not None:
            if os.path.isdir(save_into):
                os.makedirs(f"{save_into}-{db_type}", exist_ok=True)
            else:
                logger.error('Provided path is not dir or does\'t exist!')
        else:
            self.save_into = f'backup/{database_name}-mysql'
            os.makedirs(self.save_into, exist_ok=True)

    def backup_database(self) -> str:
        """Backups and writes to file with type match"""
        match self.op_type:
            case 'structure':
                now = datetime.now()
                timestamp = int(now.timestamp())
                with open(f'{self.save_into}/{timestamp}-structure.sql', 'w') as f:
                    f.write(self.backup_structure())
                return 'Success'
            case 'data':
                now = datetime.now()
                timestamp = int(now.timestamp())
                with open(f'{self.save_into}/{timestamp}-data.sql', 'w') as f:
                    f.write(self.backup_data())
                return 'Success'

            case _:
                now = datetime.now()
                timestamp = int(now.timestamp())
                if self.is_save_multiple:
                    result = f'{self.backup_structure()}\n\n\n'
                    with open(f'{self.save_into}/{timestamp}-full.DDL.sql', 'w') as f:
                        f.write(result)
                    result = f'{self.backup_data()}'
                    with open(f'{self.save_into}/{timestamp}-full.DML.sql', 'w') as f:
                        f.write(result)
                else:
                    result = f'{self.backup_structure()}\n\n\n--Data--\n' + f'{self.backup_data()}'
                    with open(f'{self.save_into}/{timestamp}-full.sql', 'w') as f:
                        f.write(result)
                return 'Success'

    def backup_structure(self) -> str:
        if self.db_type == 'mysql':
            db = mysql.MySQL(connection_string=self.connection_string, database_name=self.database_name)
            return db.get_database_structure()

    def backup_data(self) -> str:
        if self.db_type == 'mysql':
            db = mysql.MySQL(connection_string=self.connection_string, database_name=self.database_name)
            return db.get_database_data()

    def backup_table(self) -> str:
        if self.db_type == 'mysql':
            db = mysql.MySQL(connection_string=self.connection_string, database_name=self.database_name,
                             table_name=self.table_name)
            return db.get_table()
