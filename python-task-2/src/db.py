import sqlalchemy as sa


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
