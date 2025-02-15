import os
import logging

from src.models import mysql_database as mysql
from src.models import postgresql_database as postgresql

logger = logging.getLogger(__name__)


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
        logger.info(f'Database server is {self.db_type}')


    def restore_sql(self, sql: str) -> str:
        db = None
        if self.db_type == 'mysql':
            db = mysql.mysql(connection_string=self.connection_string, database_name=self.database_name)
        elif self.db_type == 'postgresql':
            db = postgresql.postgresql(connection_string=self.connection_string, database_name=self.database_name)
        return db.restore_database_sql(sql=sql)

    def restore_database(self):
        """
        Delegates restoration tasks to another methods
        """
        version_folder = f'{self.file}/{self.backup_version}'
        logger.debug(f'Backups folder is: {version_folder}')
        files = []
        if self.table_name:
            logger.info('Table is set, searching folder for table backup.')
            for file in os.listdir(version_folder):
                if self.table_name in file:
                    files.append(self.table_name)
        else:
            logger.info('Table is not set, selecting all files in directory.')
            files = os.listdir(version_folder)
        logger.debug(f'Found files in {version_folder}: {files}')
        sql_types = {
            "ddl": [],
            "dml": [],
            "dcl": []
        }

        if self.restore_type:
            match self.restore_type:
                case 'structure':
                    for file in files:
                        logger.debug(f'Processing {file}')
                        if "DDL" in file:
                            sql_types["ddl"].append(file)
                        elif "DML" in file:
                            continue
                        elif "DCL" in file:
                            continue
                        else:
                            with open(version_folder + '/' + file, 'r') as f:
                                sql = f.read()
                            self.restore_sql(sql=sql)
                        for sql_file in sql_types["ddl"]:
                            with open(version_folder + '/' + sql_file, 'r') as f:
                                sql = f.read()
                            self.restore_sql(sql=sql)
                    return f"Restored {self.database_name} database structure"
                case 'data':
                    for file in files:
                        logger.debug(f'Processing {file}')
                        if "DDL" in file:
                            continue
                        elif "DML" in file:
                            sql_types["dml"].append(file)
                        elif "DCL" in file:
                            continue
                        else:
                            with open(version_folder + '/' + file, 'r') as f:
                                sql = f.read()
                            self.restore_sql(sql=sql)
                        for sql_file in sql_types["dml"]:
                            with open(version_folder + '/' + sql_file, 'r') as f:
                                sql = f.read()
                                self.restore_sql(sql=sql)
                    return f"Restored {self.database_name} database data"

        else:
            for file in files:
                logger.debug(f'Processing {file}')
                if "DDL" in file:
                    sql_types["ddl"].append(file)
                elif "DML" in file:
                    sql_types["dml"].append(file)
                elif "DCL" in file:
                    sql_types["dcl"].append(file)
                else:
                    with open(version_folder + '/' + file, 'r') as f:
                        sql = f.read()
                    self.restore_sql(sql=sql)

            for sql_file in sql_types["ddl"]:
                with open(version_folder + '/' + sql_file, 'r') as f:
                    sql = f.read()
                self.restore_sql(sql=sql)

            for sql_file in sql_types["dml"]:
                with open(version_folder + '/' + sql_file, 'r') as f:
                    sql = f.read()
                self.restore_sql(sql=sql)

            for sql_file in sql_types["dcl"]:
                with open(version_folder + '/' + sql_file, 'r') as f:
                    sql = f.read()
                self.restore_sql(sql=sql)

            return f"Restored {self.database_name} database"


