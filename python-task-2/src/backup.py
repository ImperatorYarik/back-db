import os
from datetime import datetime
import logging

from src.models import mysql_database as mysql

from src.models import postgresql_database as postgresql

logger = logging.getLogger(__name__)


class Backup:

    def __init__(self, db_type: str, database_name: str, connection_string: str, table_name: str = None,
                 op_type: str = None,
                 is_save_multiple: bool = False, save_into: str = None) -> None:
        self.db_type = db_type
        self.database_name = database_name
        self.connection_string = connection_string
        self.table_name = table_name
        self.op_type = op_type
        self.is_save_multiple = is_save_multiple
        self.save_into = save_into
        if self.save_into is not None:
            if os.path.isdir(save_into):
                os.makedirs(f"{save_into}-{db_type}", exist_ok=True)
            else:
                logger.error('Provided path is not dir or does\'t exist!')
        else:
            self.save_into = f'backup/{database_name}-{db_type}'
            os.makedirs(self.save_into, exist_ok=True)

    def backup_database(self) -> str:
        """
        Writes sql code in files and operates backup format
        :return: Notification string
        """
        now = datetime.now()
        timestamp = int(now.timestamp())
        save_into = f"{self.save_into}/{timestamp}"
        os.makedirs(save_into, exist_ok=True)

        match self.op_type:
            case 'structure':
                if self.table_name:
                    with open(f'{save_into}/{self.table_name}.DDL.sql', 'w') as f:
                        f.write(self.backup_table())
                    return f'Successfully backed up {self.table_name}\'s {self.op_type} from {self.database_name}!'

                with open(f'{save_into}/{self.database_name}-structure.sql', 'w') as f:
                    f.write(self.backup_structure())

                return f'Successfully backed up {self.database_name}\'s {self.op_type}!'

            case 'data':
                if self.table_name:
                    with open(f'{save_into}/{self.table_name}.DML.sql', 'w') as f:
                        f.write(self.backup_table_data())
                    return f'Successfully backed up {self.table_name}\'s {self.op_type} from {self.database_name}!'

                with open(f'{save_into}/{self.database_name}-data.sql', 'w') as f:
                    f.write(self.backup_data())

                return f'Successfully backed up {self.database_name}\'s {self.op_type}!'

            case _:
                if self.is_save_multiple:
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
                    return f'Successfully backed up {self.database_name}!'

                else:
                    if self.table_name:
                        result = f'{self.backup_table()}\n\n\n-- DATA --\n' + f'{self.backup_table_data()}'
                        with open(f'{save_into}/{self.table_name}.sql', 'w') as f:
                            f.write(result)

                        return f'Successfully backed up {self.table_name} from {self.database_name}!'

                    result = f'{self.backup_structure()}\n\n\n-- DATA --\n' + f'{self.backup_data()}'
                    with open(f'{save_into}/{self.database_name}.sql', 'w') as f:
                        f.write(result)

                    return f'Successfully backed up {self.database_name}!'

    def backup_structure(self) -> str:
        """
        Connects to database and returns schema
        :return: sql code
        """
        if self.db_type == 'mysql':
            db = mysql.mysql(connection_string=self.connection_string, database_name=self.database_name)
            return db.get_database_structure()

        elif self.db_type == 'postgresql':
            db = postgresql.postgresql(connection_string=self.connection_string, database_name=self.database_name)
            return db.get_database_structure()

    def backup_data(self) -> str:
        """
        Connects to database and returns insert data
        :return: sql code
        """
        if self.db_type == 'mysql':
            db = mysql.mysql(connection_string=self.connection_string, database_name=self.database_name)
            return db.get_database_data()

        elif self.db_type == 'postgresql':
            db = postgresql.postgresql(connection_string=self.connection_string, database_name=self.database_name)
            return db.get_database_data()

    def backup_table(self) -> str:
        """
        Connects to database and returns table creation sql
        :return: sql code
        """
        if self.db_type == 'mysql':
            db = mysql.mysql(connection_string=self.connection_string, database_name=self.database_name,
                             table_name=self.table_name)
            return db.get_table()

        elif self.db_type == 'postgresql':
            db = postgresql.postgresql(connection_string=self.connection_string, database_name=self.database_name)
            return db.get_table()

    def backup_table_data(self) -> str:
        """
        Connects to database and returns table insert data
        :return: sql code
        """
        if self.db_type == 'mysql':
            db = mysql.mysql(connection_string=self.connection_string, database_name=self.database_name,
                             table_name=self.table_name)
            return db.get_table_data()

        elif self.db_type == 'postgresql':
            db = postgresql.postgresql(connection_string=self.connection_string, database_name=self.database_name)
            return db.get_table_data()

    def backup_separate(self) -> dict:
        """
        Connects to database and returns all sql data in comfortable format with separate sql code types (DDL, DML, DCL)
        :return: dictionary of dictionaries {type}{table}[sql code]
        """
        db = None

        if self.db_type == 'mysql':
            db = mysql.mysql(connection_string=self.connection_string, database_name=self.database_name,
                             table_name=self.table_name)

        elif self.db_type == 'postgresql':
            db = postgresql.postgresql(connection_string=self.connection_string, database_name=self.database_name,
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
