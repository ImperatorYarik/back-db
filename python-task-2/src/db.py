import sqlalchemy as sa
from sqlalchemy import text
from sqlalchemy.engine import row


class Database():
    def __init__(self, database_url: str, database_name: str):
        self.database_name = database_name
        self.engine = sa.create_engine(database_url)
        self.metadata = sa.MetaData()
        self.metadata.reflect(bind=self.engine)

    def get_database_schema(self):
        schema = {}

        # Iterate over the tables in the MetaData
        for table_name, table in self.metadata.tables.items():
            columns = []
            for column in table.columns:
                column_info = {
                    "name": column.name,
                    "type": str(column.type),
                    "nullable": column.nullable,
                    "primary_key": column.primary_key,
                    "default": column.default
                }
                columns.append(column_info)
            schema[table_name] = columns

        return schema

    def get_all_data(self):
        data = {}
        with self.engine.connect() as connection:
            for table_name in self.metadata.tables:
                query = text(f'SELECT * FROM {table_name}')
                result_set = connection.execute(query)
                rows = result_set.fetchall()

                columns = result_set.keys()
                table_data = [dict(zip(columns, row)) for row in rows]
                data[table_name] = table_data

            return data
        pass
