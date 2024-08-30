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
        now = datetime.now()
        timestamp = int(now.timestamp())
        save_into = f"{self.save_into}/{timestamp}"
        os.makedirs(save_into, exist_ok=True)

        match self.op_type:
            case 'structure':

                if self.table_name:
                    with open(f'{save_into}/{self.table_name}.DDL.sql', 'w') as f:
                        f.write(self.backup_table())
                    return 'Success'
                with open(f'{save_into}/{self.database_name}-structure.sql', 'w') as f:
                    f.write(self.backup_structure())
                return 'Success'
            case 'data':

                if self.table_name:
                    with open(f'{save_into}/{self.table_name}.DML.sql', 'w') as f:
                        f.write(self.backup_table_data())
                    return 'Success'
                with open(f'{save_into}/{self.database_name}-data.sql', 'w') as f:
                    f.write(self.backup_data())
                return 'Success'

            case _:
                if self.is_save_multiple:
                    # TODO: Make separate backup (Get list of all tables and execute methods)
                    sql_dict = self.backup_separate()
                    for table, structure in sql_dict["ddl"].items():
                        with open(f'{save_into}/{table}.DDL.sql', 'w') as f:
                            f.write(structure)
                    for table, structure in sql_dict["dcl"].items():
                        with open(f'{save_into}/{table}.DCL.sql', 'w') as f:
                            f.write(structure)
                    for db, structure in sql_dict["dml"].items():
                        with open(f'{save_into}/{db}.DML.sql', 'w') as f:
                            f.write(structure)
                    return 'Success'

                else:
                    if self.table_name:
                        result = f'{self.backup_table()}\n\n\n-- DATA --\n' + f'{self.backup_table_data()}'
                        with open(f'{save_into}/{self.table_name}.sql', 'w') as f:
                            f.write(result)
                        return 'Success'
                    result = f'{self.backup_structure()}\n\n\n-- DATA --\n' + f'{self.backup_data()}'
                    with open(f'{save_into}/{self.database_name}.sql', 'w') as f:
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
    def backup_table_data(self) -> str:
        if self.db_type == 'mysql':
            db = mysql.MySQL(connection_string=self.connection_string, database_name=self.database_name,
                             table_name=self.table_name)
            return db.get_table_data()

    def backup_separate(self) -> dict:
        if self.db_type == 'mysql':
            db = mysql.MySQL(connection_string=self.connection_string, database_name=self.database_name,
                             table_name=self.table_name)
            tables = db.get_all_tables()
            result = {
                "ddl": {},
                "dml": {},
                "dcl": {}
            }

            for table in tables:
                result["ddl"][table] = db.get_table(custom_table=table)
                result["dml"][table] = db.get_table_data(custom_table=table)
            result["dcl"][self.database_name] = db.get_grants()
            return result
