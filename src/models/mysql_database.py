import binascii
import datetime
import logging

import pymysql

from src.models.database import database

mysql_version = 80003
logger = logging.getLogger(__name__)

def parse_connection_string(connection_string: str) -> dict:
    """
    Parses mysql connection string in pymysql acceptable format
    :param connection_string: connection string to parse
    :return: dictionary of connection data (user, pass, host, port, database)
    """
    import re
    pattern = r'mysql://(?P<user>[^:]+):(?P<password>[^@]+)@(?P<host>[^:]+):(?P<port>\d+)/'
    logger.debug('Parsing connection string...')
    match = re.match(pattern, connection_string)
    if not match:
        raise ValueError("Invalid connection string format")
    return match.groupdict()


class mysql(database):
    def __init__(self, database_name: str, connection_string: str, is_restore: bool = False,
                 table_name: str = None) -> None:
        self.database_name = database_name
        self.connection_string = connection_string
        self.table_name = table_name
        self.is_restore = is_restore
        connection_params = parse_connection_string(connection_string)
        try:
            self.connection = pymysql.connect(
                user=connection_params['user'],
                password=connection_params['password'],
                host=connection_params['host'],
                port=int(connection_params['port']),
                database=database_name,
                charset='utf8mb4'
            )
        except Exception as e:
            logger.warning(e)
            if not self.is_restore:
                logger.warning(f'Creating database {self.database_name}')
                self.connection = pymysql.connect(
                    user=connection_params['user'],
                    password=connection_params['password'],
                    host=connection_params['host'],
                    port=int(connection_params['port']),
                    charset='utf8mb4'
                )
                cursor = self.connection.cursor()
                cursor.execute(f'CREATE SCHEMA {self.database_name}')

        self.turn_off_checks_sql = f"""SET NAMES utf8mb4;
SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='TRADITIONAL';

"""
        self.turn_off_checks_tables_sql = """SET NAMES utf8mb4;
SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='TRADITIONAL';
SET @old_autocommit=@@autocommit;"""

        self.turn_on_checks_sql = """SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
"""

    def get_all_tables(self) -> list:
        """
        Returns a list of all tables in the database
        :return:
        """
        cursor = self.connection.cursor()
        sql_query = f"SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = '{self.database_name}'"
        cursor.execute(sql_query)
        logger.debug(f'Getting all database tables.')
        return [row[0] for row in cursor.fetchall()]

    def get_database_structure(self) -> str:
        """
        Creates a string sql code for mysql database schema creation with all tables
        :return: sql code represented as string
        """

        tables = self.get_all_tables()

        structure = self.turn_off_checks_sql + f"""DROP SCHEMA IF EXISTS {self.database_name};
CREATE SCHEMA {self.database_name};
USE {self.database_name};"""

        for table in tables:
            structure += self.get_table(custom_table=table)
        structure += self.get_grants()
        structure += self.turn_on_checks_sql
        return structure

    def get_database_data(self) -> str:
        """
        Creates a string sql code for mysql database data insertion for all tables
        :return: sql code represented as string
        """
        cursor = self.connection.cursor()
        result = self.turn_off_checks_tables_sql
        result += f'USE {self.database_name};\n'
        logger.info('Getting database data...')
        try:
            cursor.execute(f"SHOW FULL TABLES WHERE Table_Type = 'BASE TABLE'")
            tables = [row[0] for row in cursor.fetchall()]

            for table in tables:
                result += self.get_table_data(custom_table=table)


        except pymysql.Error as err:
            logger.error(err)
            raise Exception(err)

        return result

    def get_table(self, custom_table: str = None) -> str:
        """
        Creates a string sql code for mysql one table creation
        :param custom_table: if class param is not set
        :return: a sql code represented as string
        """
        structure = ''
        if custom_table is not None:
            table = custom_table
        else:
            table = self.table_name
            structure += self.turn_off_checks_sql

        logger.info(f'Getting structure of {table}')

        cursor = self.connection.cursor()
        cursor.execute(f'SHOW CREATE TABLE `{table}`')
        create_table = cursor.fetchall()
        structure += create_table[0][1] + ';\n'
        if self.table_name:
            structure += self.turn_off_checks_sql

        return structure

    def get_table_data(self, custom_table: str = None) -> str:
        """
        Creates a string sql code for mysql one table data insertion
        :param custom_table: if class param is not set
        :return: a sql code represented as string
        """
        cursor = self.connection.cursor()
        if custom_table is not None:
            table = custom_table
            result = ''
        else:
            result = f'USE {self.database_name};\n'
            table = self.table_name

        logger.info(f'Getting data from table: {table}')

        cursor.execute(f"SELECT * FROM {table}")
        rows = cursor.fetchall()

        result += f"SET AUTOCOMMIT=0;\nINSERT INTO `{table}` ({', '.join([desc[0] for desc in cursor.description])}) VALUES "

        for row in rows:
            formatted_data = ''
            for value in row:
                if value is None:
                    formatted_data += 'NULL'
                elif isinstance(value, str):
                    formatted_data += f'\'{value}\''
                elif isinstance(value, datetime.datetime):
                    formatted_data += f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'"
                elif isinstance(value, bytes):
                    hex_value = binascii.hexlify(value).decode('ascii')
                    formatted_data += f'/*!{mysql_version} 0x{hex_value}*/'
                else:
                    formatted_data += f'{value}'
                formatted_data += ','
            formatted_data = formatted_data[:-1]
            result += f'({formatted_data}),\n'

        if result.endswith(',\n'):
            result = result[:-2] + ';\nCOMMIT;\n'
        return result

    def get_grants(self) -> str:
        """
        Creates a string sql code for mysql grants creation
        :return: a sql code represented as string
        """
        cursor = self.connection.cursor()
        result = ''

        logger.info(f'Getting grants from {self.database_name}')

        cursor.execute(f""" SELECT DISTINCT USER, HOST 
                FROM mysql.db 
                WHERE DB = '{self.database_name}'""")
        users = cursor.fetchall()
        for user, host in users:
            cursor.execute(f"SHOW GRANTS FOR `{user}`@`{host}`;")
            grants = cursor.fetchall()
            for grant in grants:
                result += f"\n{grant[0]};"
        return result

    def restore_database_sql(self, sql: str) -> bool:
        """
        Executes sql script
        :param sql: sql code which needs to be executed
        :return: True if success
        """
        cursor = self.connection.cursor()
        sql = self.turn_off_checks_sql + self.turn_off_checks_tables_sql + f'USE {self.database_name};' + sql + self.turn_on_checks_sql

        logger.debug('Executing sql script...')
        structure = list(filter(None, sql.split(';')))
        for element in structure:
            try:
                cursor.execute(f'{element};')
            except Exception as e:
                logger.debug(e)

        return True
