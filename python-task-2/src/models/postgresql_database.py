import logging
import datetime

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


class postgresql(database):
    def __init__(self, database_name: str, connection_string: str, is_restore: bool = False,
                 table_name: str = None) -> None:
        self.database_name = database_name
        self.connection_string = connection_string
        self.table_name = table_name
        self.is_restore = is_restore
        connection_params = parse_connection_string(connection_string)
        try:
            self.connection = psycopg2.connect(
                user=connection_params['user'],
                password=connection_params['password'],
                host=connection_params['host'],
                port=int(connection_params['port']),
                dbname=database_name,
            )
        except Exception as e:
            logger.warning(e)
            if not self.is_restore:
                logger.warning(f'Creating database {self.database_name}')
                temp_connection = psycopg2.connect(
                    user=connection_params['user'],
                    password=connection_params['password'],
                    host=connection_params['host'],
                    port=int(connection_params['port']),
                    dbname='postgres'
                )
                temp_connection.autocommit = True
                temp_cursor = temp_connection.cursor()
                temp_cursor.execute(f'CREATE DATABASE {self.database_name}')
                temp_cursor.close()
                temp_connection.close()

                self.connection = psycopg2.connect(
                    user=connection_params['user'],
                    password=connection_params['password'],
                    host=connection_params['host'],
                    port=int(connection_params['port']),
                    dbname=database_name,
                )

    def get_all_tables(self) -> list:
        """
        Returns a list of all tables in the database
        :return:
        """
        logger.debug(f'Getting all database tables.')

        cursor = self.connection.cursor()
        cursor.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                      AND table_type = 'BASE TABLE';
                """)
        return [row[0] for row in cursor.fetchall()]

    def get_database_structure(self) -> str:
        """
        Creates a string sql code for mysql database schema creation with all tables
        :return: sql code represented as string
        """

        create_database_script = """SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;



SET default_tablespace = '';

SET default_with_oids = false;\n\n"""
        tables = self.get_all_tables()
        for table in tables:
            create_database_script += f'DROP TABLE IF EXISTS {table};\n'

        create_database_script += '\n\n'
        for table in tables:
            create_database_script += self.get_table(custom_table=table) + '\n\n'

        return create_database_script

    def get_database_data(self) -> str:
        """
        Creates a string sql code for mysql database data insertion for all tables
        :return: sql code represented as string
        """
        logger.info('Getting database data...')

        tables = self.get_all_tables()
        result = ''

        for table in tables:
            result += self.get_table_data(custom_table=table) + '\n\n'

        return result

    def get_table(self, custom_table: str = None) -> str:
        """
        Creates a string sql code for mysql one table creation
        :param custom_table: if class param is not set
        :return: a sql code represented as string
        """

        cursor = self.connection.cursor()
        if custom_table is not None:
            table = custom_table
        else:
            table = self.table_name
        logger.info(f'Getting structure of {table}')

        cursor.execute(f"""
                    SELECT 
                        column_name, 
                        data_type, 
                        is_nullable, 
                        column_default,
                        character_maximum_length
                    FROM 
                        information_schema.columns 
                    WHERE 
                        table_name = %s
                    ORDER BY 
                        ordinal_position;
                """, (table,))

        columns = cursor.fetchall()
        create_table_script = f"CREATE TABLE {table} (\n"

        for column in columns:
            column_name, data_type, is_nullable, column_default, char_max_length = column
            col_def = f"\t{column_name} {data_type.upper()}"

            if char_max_length is not None:
                col_def += f"({char_max_length})"

            if column_default is not None:
                col_def += f" DEFAULT {column_default}"

            if is_nullable == "NO":
                col_def += " NOT NULL"

            create_table_script += col_def + ",\n"

        create_table_script = create_table_script.rstrip(",\n") + "\n);"

        cursor.execute(f"""
                    SELECT 
                        pg_index.indisprimary,
                        pg_catalog.pg_get_indexdef(pg_index.indexrelid)
                    FROM 
                        pg_catalog.pg_class c, 
                        pg_catalog.pg_index pg_index 
                    WHERE 
                        c.oid = pg_index.indrelid 
                        AND c.relname = %s;
                """, (table,))

        indexes = cursor.fetchall()

        for is_primary, index_def in indexes:
            if is_primary:
                create_table_script += f"\n{index_def};"

        return create_table_script

    def get_table_data(self, custom_table: str = None) -> str:
        """
        Creates a string sql code for mysql one table data insertion
        :param custom_table: if class param is not set
        :return: a sql code represented as string
        """
        import re
        cursor = self.connection.cursor()

        if custom_table is not None:
            table = custom_table
        else:
            table = self.table_name

        cursor.execute(f"SELECT * FROM {table}")
        rows = cursor.fetchall()

        date_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}$')
        # TODO: add column names
        insert_statement = f'INSERT INTO {table} ({', '.join([desc[0] for desc in cursor.description])}) VALUES '
        result = ''
        for row in rows:
            values = ''
            for value in row:
                if isinstance(value, str):
                    value = value.replace("'", "''")
                    values += f"'{value}'"
                elif value is None:
                    values += "NULL"
                elif date_pattern.match(str(value)):
                    values += f"\'{value}\'"
                elif isinstance(value, memoryview):
                    values += f"'\\x{value.tobytes().hex()}'"
                else:
                    values += str(value)

                values += ','
            result += f'({values[:-1]}),\n'
        if result.endswith(',\n'):
            result = result[:-2] + ';\n'

        if result == '':
            return ''
        result = insert_statement + result

        return result

    def get_grants(self) -> str:
        """
        Creates a string sql code for mysql grants creation
        :return: a sql code represented as string
        """

        cursor = self.connection.cursor()

        cursor.execute("""
                    SELECT
                        pg_roles.rolname AS grantee,
                        pg_database.datname AS database,
                        has_database_privilege(pg_roles.rolname, pg_database.datname, 'CONNECT') AS can_connect,
                        has_database_privilege(pg_roles.rolname, pg_database.datname, 'CREATE') AS can_create,
                        has_database_privilege(pg_roles.rolname, pg_database.datname, 'TEMP') AS can_temp
                    FROM pg_roles, pg_database
                    WHERE pg_roles.rolcanlogin = TRUE;
                """)

        database_privileges = cursor.fetchall()
        grant_statements = ''

        for row in database_privileges:
            grantee, database, can_connect, can_create, can_temp = row
            if can_connect:
                grant_statements += f"GRANT CONNECT ON DATABASE {database} TO {grantee};\n"
            if can_create:
                grant_statements += f"GRANT CREATE ON DATABASE {database} TO {grantee};\n"
            if can_temp:
                grant_statements += f"GRANT TEMP ON DATABASE {database} TO {grantee};\n"

        cursor.execute("""
                    SELECT grantee, privilege_type, table_name
                    FROM information_schema.table_privileges
                    WHERE table_schema = 'public';  -- Change schema as needed
                """)

        table_privileges = cursor.fetchall()
        for row in table_privileges:
            grantee, privilege_type, table_name = row
            grant_statements += f"GRANT {privilege_type} ON TABLE {table_name} TO {grantee};\n"

        return grant_statements

    def restore_database_sql(self, sql: str) -> bool:
        """
        Executes sql script
        :param sql: sql code which needs to be executed
        :return: True if success
        """

        cursor = self.connection.cursor()
        logger.debug('Executing sql script...')
        structure = filter(None, sql.split(';'))
        for element in structure:
            try:
                cursor.execute(element)
            except Exception as e:
                logger.warning(e)

        self.connection.commit()

        return True
