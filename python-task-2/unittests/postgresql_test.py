import unittest

from unittest.mock import MagicMock, patch

from src.models import postgresql_database as postgresql_database

class TestPostgreSQLDatabase(unittest.TestCase):

    @patch('postgresql_database.psycopg2.connect')
    def test_get_all_tables(self, mock_connect):
        db = postgresql_database.postgresql(database_name='test', connection_string='postgresql://postgres:postgres@localhost:5432/test')

        mock_cursor = MagicMock()
        mock_connect.return_value.cursor.return_value = mock_cursor

        mock_cursor.fetchall.return_value = [('categories',), ('products',), ('orders',)]

        result = db.get_all_tables()

        self.assertEqual(result, ['categories', 'products', 'orders'])

        mock_cursor.execute.assert_called_once_with("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                      AND table_type = 'BASE TABLE';
                """)


if __name__ == '__main__':
    unittest.main()
