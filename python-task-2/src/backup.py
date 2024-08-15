import os
from datetime import datetime
import logging

from src.models import dbmysql as mysql


class Backup:

    def __init__(self, db_type: str, database_name: str, connection_string: str, table_name: str = None,
                 type: str = None,
                 is_save_one: bool = False, is_save_multiple: bool = False, save_into: str = None) -> None:
        logger = logging.getLogger(__name__)
        self.db_type = db_type
        self.database_name = database_name
        self.connection_string = connection_string
        self.table_name = table_name
        self.type = type
        self.is_save_one = is_save_one
        self.is_save_multiple = is_save_multiple
        self.save_into = save_into
        if self.save_into is not None:
            if os.path.isdir(save_into):
                os.makedirs(save_into, exist_ok=True)
            else:
                logger.error('Provided path is not dir or does\'t exist!')
        else:
            self.save_into = 'backup'
            os.makedirs(self.save_into, exist_ok=True)

    def backup_database(self) -> None:
        """Delegates backup tasks to another methods"""
        match self.db_type:
            case 'structure':
                now = datetime.now()
                timestamp = now.timestamp()
                with open(f'{self.save_into}/{timestamp}-structure.sql', 'w') as f:
                    f.write(self.backup_structure())
            case 'data':
                pass
            case _:
                result = f'{self.backup_structure()}\n\n\n' + f'{self.backup_data()}'
                return result

    def backup_structure(self):
        if self.db_type == 'mysql':
            db = mysql.MySQL(connection_string=self.connection_string, database_name=self.database_name)
            return db.get_database_structure()

    def backup_data(self):
        pass
