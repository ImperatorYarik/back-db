import binascii
import datetime
import logging

import psycopg2

from src.models.database import database

logger = logging.getLogger(__name__)


def parse_connection_string(connection_string: str) -> dict:
    """
    Parses mysql connection string in pymysql acceptable format
    :param connection_string: connection string to parse
    :return: dictionary of connection data (user, pass, host, port, database)
    """
    import re
    pattern = r'postgresql://(?P<user>[^:]+):(?P<password>[^@]+)@(?P<host>[^:]+):(?P<port>\d+)/'
    logger.debug('Parsing connection string...')
    match = re.match(pattern, connection_string)
    if not match:
        raise ValueError("Invalid connection string format")
    return match.groupdict()


class Postgresql(database):
    def __init__(self, database_name: str, connection_string: str, table_name: str = None) -> None:
        self.database_name = database_name
        self.connection_string = connection_string
        self.table_name = table_name
        connection_params = parse_connection_string(connection_string)
        try:
            self.connection = psycopg2.connect(
                user=connection_params['user'],
                password=connection_params['password'],
                host=connection_params['host'],
                port=int(connection_params['port']),
                database=database_name,
                charset='utf8mb4'
            )
        except Exception as e:
            logger.warning(e)

    def get_all_tables(self) -> list:
        """Get all tables in the database."""
        pass

    def get_database_structure(self) -> str:
        """Get database structure"""
        pass

    def get_database_data(self) -> str:
        """Get database data"""
        pass

    def get_table(self, custom_table: str) -> str:
        """Get table structure with data"""
        pass

    def get_table_data(self, custom_table: str) -> str:
        """Get table structure with data"""
        pass

    def get_grants(self) -> str:
        """Get grants"""
        pass

    def restore_database_sql(self, database_data) -> bool:
        """Restore database structure"""
        pass

