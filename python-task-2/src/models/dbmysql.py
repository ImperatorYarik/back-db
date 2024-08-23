import binascii
import datetime

import networkx as nx

import pymysql

from src.models.database import Database

mysql_version = 80003


class MySQL(Database):
    def __init__(self, database_name: str, connection_string: str, table_name: str = None) -> None:
        self.database_name = database_name
        self.connection_string = connection_string
        self.table_name = table_name
        connection_params = self.parse_connection_string(connection_string)

        self.connection = pymysql.connect(
            user=connection_params['user'],
            password=connection_params['password'],
            host=connection_params['host'],
            port=int(connection_params['port']),
            database=database_name,
            charset='utf8mb4'
        )

    def get_database_structure(self) -> str:
        """
        Returns database create structure sql code in string format
        """
        cursor = self.connection.cursor()
        sql_query = f"SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = '{self.database_name}'"
        cursor.execute(sql_query)
        tables = [row[0] for row in cursor.fetchall()]

        structure = f"""SET NAMES utf8mb4;
SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='TRADITIONAL';

DROP SCHEMA IF EXISTS {self.database_name};
CREATE SCHEMA {self.database_name};
USE {self.database_name};"""
        for table in tables:
            structure += self.get_table(custom_table=table)
        structure += """SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
"""
        return structure

    # FIXME: bad return format, need to change list to string, with correct sql format
    def get_database_data(self) -> str:
        """
        Returns data insert sql code in string format
        """
        cursor = self.connection.cursor()
        result = """SET NAMES utf8mb4;
SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='TRADITIONAL';
SET @old_autocommit=@@autocommit;"""
        result += f'USE {self.database_name};\n\n'

        try:
            cursor.execute(f"SHOW FULL TABLES WHERE Table_Type = 'BASE TABLE'")
            tables = [row[0] for row in cursor.fetchall()]

            for table in tables:
                result += self.get_table_data(custom_table=table)


        except pymysql.Error as err:
            raise Exception(err)

        return result

    def get_table(self, custom_table: str = None) -> str:
        structure = ''
        if custom_table is not None:
            table = custom_table
        else:
            table = self.table_name
            structure += f"""SET NAMES utf8mb4;
SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='TRADITIONAL';

DROP SCHEMA IF EXISTS {self.database_name};
CREATE SCHEMA {self.database_name};
USE {self.database_name};\n"""
            structure += """SET NAMES utf8mb4;
SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='TRADITIONAL';
SET @old_autocommit=@@autocommit;"""
        cursor = self.connection.cursor()
        cursor.execute(f'SHOW CREATE TABLE `{table}`')
        create_table = cursor.fetchall()
        structure += create_table[0][1] + ';\n\n'
        if self.table_name:
            structure += """SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
SET NAMES utf8mb4;
SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='TRADITIONAL';
SET @old_autocommit=@@autocommit;
"""

        return structure

    def get_table_data(self, custom_table: str) -> str:
        cursor = self.connection.cursor()
        result = ''
        result += f"SET AUTOCOMMIT=0;\nINSERT INTO `{custom_table}` VALUES "
        cursor.execute(f"SELECT * FROM {custom_table}")
        rows = cursor.fetchall()
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
            result = result[:-2] + ';\nCOMMIT;\n\n'
        return result

    def restore_database_structure(self, database_structure: str) -> bool:
        cursor = self.connection.cursor()
        structure = database_structure.split(';')
        for element in structure:
            # print(element)
            try:
                cursor.execute(element)
            except Exception as e:
                print(e)

        return True

    def restore_database_data(self, database_data) -> bool:
        cursor = self.connection.cursor()
        data = database_data.split(';')
        for element in data:
            try:
                cursor.execute(element)
            except Exception as e:
                print(e)

        return True

    def restore_table(self, table_data) -> bool:
        pass

    def parse_connection_string(self, connection_string: str) -> dict:
        import re
        pattern = r'mysql://(?P<user>[^:]+):(?P<password>[^@]+)@(?P<host>[^:]+):(?P<port>\d+)/'
        match = re.match(pattern, connection_string)
        if not match:
            raise ValueError("Invalid connection string format")
        return match.groupdict()
