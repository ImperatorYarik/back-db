import unittest
from unittest.mock import patch, mock_open, MagicMock
import os
from src.restore import Restore  # Assuming your class is stored in restore.py


class TestRestore(unittest.TestCase):

    @patch('os.listdir')
    def test_restore_init_without_backup_version(self, mock_listdir):
        # Mocking os.listdir to return some backup versions
        mock_listdir.return_value = ['backup_v1', 'backup_v2']

        # Create Restore object without specifying backup_version
        restore = Restore(database_name='test_db', connection_string='test_connection', db_type='mysql')

        # Since backup_version is not provided, it should select the max version
        self.assertEqual(restore.backup_version, 'backup_v2')
        mock_listdir.assert_called_once_with(f'backup/test_db-mysql')

    @patch('os.listdir')
    def test_restore_init_with_backup_version(self, mock_listdir):
        # Create Restore object with a specific backup_version
        restore = Restore(database_name='test_db', connection_string='test_connection', db_type='mysql', backup_version='backup_v1')

        # Ensure the backup_version is correctly set
        self.assertEqual(restore.backup_version, 'backup_v1')

        # os.listdir should not be called when backup_version is provided
        mock_listdir.assert_not_called()

    @patch('builtins.open', new_callable=mock_open)
    @patch('os.listdir')
    @patch.object(Restore, 'restore_sql')
    def test_restore_database_structure(self, mock_restore_sql, mock_listdir, mock_file):
        # Mock os.listdir to simulate backup files with timestamped versions
        mock_listdir.side_effect = [
            ['1690000000', '1691000000'],  # First call simulates backup versions (timestamps)
            ['test_db-structure.DDL.sql', 'test_db-data.DML.sql']
            # Second call simulates files in the latest backup version
        ]

        # Create Restore object with restore_type='structure'
        restore = Restore(database_name='test_db', connection_string='test_connection', db_type='mysql',
                          restore_type='structure')

        # Simulate reading from the file
        mock_file.return_value.read.return_value = 'CREATE TABLE test;'

        # Call restore_database and check behavior
        result = restore.restore_database()

        # Check if the correct SQL file was read and the SQL command was passed to restore_sql
        mock_file.assert_called_with('backup/test_db-mysql/1691000000/test_db-structure.DDL.sql', 'r')
        mock_restore_sql.assert_called_once_with(sql='CREATE TABLE test;')

        self.assertEqual(result, 'Restored test_db database structure')

    @patch('builtins.open', new_callable=mock_open)
    @patch('os.listdir')
    @patch.object(Restore, 'restore_sql')
    def test_restore_database_data(self, mock_restore_sql, mock_listdir, mock_file):
        # Mock os.listdir to simulate backup files with timestamped versions
        mock_listdir.side_effect = [
            ['1690000000', '1691000000'],  # First call simulates backup versions (timestamps)
            ['test_db-structure.DDL.sql', 'test_db-data.DML.sql']
            # Second call simulates files in the latest backup version
        ]

        # Create Restore object with restore_type='data'
        restore = Restore(database_name='test_db', connection_string='test_connection', db_type='mysql',
                          restore_type='data')

        # Simulate reading from the file
        mock_file.return_value.read.return_value = 'INSERT INTO test VALUES (1);'

        # Call restore_database and check behavior
        result = restore.restore_database()

        # Check if the correct SQL file was read and the SQL command was passed to restore_sql
        mock_file.assert_called_with('backup/test_db-mysql/1691000000/test_db-data.DML.sql', 'r')
        mock_restore_sql.assert_called_once_with(sql='INSERT INTO test VALUES (1);')

        self.assertEqual(result, 'Restored test_db database data')

    @patch('builtins.open', new_callable=mock_open)
    @patch('os.listdir')
    @patch.object(Restore, 'restore_sql')
    def test_restore_full_database(self, mock_restore_sql, mock_listdir, mock_file):
        # Mock os.listdir to simulate backup files
        mock_listdir.return_value = ['test_db-structure.DDL.sql', 'test_db-data.DML.sql', 'test_db-privileges.DCL.sql']

        # Create Restore object with no restore_type (full restore)
        restore = Restore(database_name='test_db', connection_string='test_connection', db_type='mysql')

        # Simulate reading from the files
        mock_file.return_value.read.side_effect = [
            'CREATE TABLE test;',  # DDL
            'INSERT INTO test VALUES (1);',  # DML
            'GRANT ALL PRIVILEGES;'  # DCL
        ]

        # Call restore_database and check behavior
        result = restore.restore_database()

        # Check that all SQL files (DDL, DML, DCL) were processed
        self.assertEqual(mock_file.call_count, 3)
        self.assertEqual(mock_restore_sql.call_count, 3)

        # Ensure the SQL from all types were passed to restore_sql
        mock_restore_sql.assert_any_call(sql='CREATE TABLE test;')
        mock_restore_sql.assert_any_call(sql='INSERT INTO test VALUES (1);')
        mock_restore_sql.assert_any_call(sql='GRANT ALL PRIVILEGES;')

        self.assertEqual(result, 'Restored test_db database')


if __name__ == '__main__':
    unittest.main()
