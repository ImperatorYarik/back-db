import unittest
from unittest.mock import patch, MagicMock

from datetime import datetime
from src.backup import Backup

class TestBackup(unittest.TestCase):

    @patch('os.makedirs')
    def test_backup_init_without_directory(self, mock_makedirs):
        backup = Backup(db_type="postgresql", database_name="test_db", connection_string="test_conn")
        mock_makedirs.assert_called_with("backup/test_db-postgresql", exist_ok=True)

    @patch('os.makedirs')
    def test_backup_database_structure_mysql(self, mock_makedirs):
        mock_makedirs.return_value = None

        backup = Backup(db_type="mysql", database_name="test_db", connection_string="test_conn", op_type="structure")
        with patch.object(backup, 'backup_structure', return_value="CREATE TABLE test;"):
            with patch('builtins.open', unittest.mock.mock_open()) as mock_file:
                result = backup.backup_database()
                self.assertEqual(result, "Successfully backed up test_db's structure!")
                mock_file.assert_called_once_with(f'backup/test_db-mysql/{int(datetime.now().timestamp())}/test_db-structure.sql', 'w')

    @patch('os.makedirs')
    def test_backup_database_structure_postgresql(self, mock_makedirs):
        mock_makedirs.return_value = None

        backup = Backup(db_type="postgresql", database_name="test_db", connection_string="test_conn", op_type="structure")
        with patch.object(backup, 'backup_structure', return_value="CREATE TABLE test;"):
            with patch('builtins.open', unittest.mock.mock_open()) as mock_file:
                result = backup.backup_database()
                self.assertEqual(result, "Successfully backed up test_db's structure!")
                mock_file.assert_called_once_with(f'backup/test_db-postgresql/{int(datetime.now().timestamp())}/test_db-structure.sql', 'w')

    @patch('os.makedirs')
    def test_backup_database_data_mysql(self, mock_makedirs):
        mock_makedirs.return_value = None

        backup = Backup(db_type="mysql", database_name="test_db", connection_string="test_conn", op_type="data")
        with patch.object(backup, 'backup_data', return_value="INSERT INTO test VALUES (1, 'data');"):
            with patch('builtins.open', unittest.mock.mock_open()) as mock_file:
                result = backup.backup_database()
                self.assertEqual(result, "Successfully backed up test_db's data!")
                mock_file.assert_called_once_with(f'backup/test_db-mysql/{int(datetime.now().timestamp())}/test_db-data.sql', 'w')

    @patch('os.makedirs')
    def test_backup_database_data_postgresql(self, mock_makedirs):
        mock_makedirs.return_value = None

        backup = Backup(db_type="postgresql", database_name="test_db", connection_string="test_conn", op_type="data")
        with patch.object(backup, 'backup_data', return_value="INSERT INTO test VALUES (1, 'data');"):
            with patch('builtins.open', unittest.mock.mock_open()) as mock_file:
                result = backup.backup_database()
                self.assertEqual(result, "Successfully backed up test_db's data!")
                mock_file.assert_called_once_with(f'backup/test_db-postgresql/{int(datetime.now().timestamp())}/test_db-data.sql', 'w')

    @patch('os.makedirs')
    def test_backup_table_structure_mysql(self, mock_makedirs):
        mock_makedirs.return_value = None

        backup = Backup(db_type="mysql", database_name="test_db", connection_string="test_conn", table_name="test_table", op_type="structure")
        with patch.object(backup, 'backup_table', return_value="CREATE TABLE test_table;"):
            with patch('builtins.open', unittest.mock.mock_open()) as mock_file:
                result = backup.backup_database()
                self.assertEqual(result, "Successfully backed up test_table's structure from test_db!")
                mock_file.assert_called_once_with(f'backup/test_db-mysql/{int(datetime.now().timestamp())}/test_table.DDL.sql', 'w')

    @patch('os.makedirs')
    def test_backup_table_structure_postgresql(self, mock_makedirs):
        mock_makedirs.return_value = None

        backup = Backup(db_type="postgresql", database_name="test_db", connection_string="test_conn", table_name="test_table", op_type="structure")
        with patch.object(backup, 'backup_table', return_value="CREATE TABLE test_table;"):
            with patch('builtins.open', unittest.mock.mock_open()) as mock_file:
                result = backup.backup_database()
                self.assertEqual(result, "Successfully backed up test_table's structure from test_db!")
                mock_file.assert_called_once_with(f'backup/test_db-postgresql/{int(datetime.now().timestamp())}/test_table.DDL.sql', 'w')

    @patch('os.makedirs')
    def test_backup_separate_mysql(self, mock_makedirs):
        mock_makedirs.return_value = None

        backup = Backup(db_type="mysql", database_name="test_db", connection_string="test_conn", is_save_multiple=True)
        mock_sql_dict = {
            "ddl": {"table1": "CREATE TABLE table1;"},
            "dml": {"table1": "INSERT INTO table1 VALUES (1);"},
            "dcl": {"test_db": "GRANT ALL PRIVILEGES;"}
        }
        with patch.object(backup, 'backup_separate', return_value=mock_sql_dict):
            with patch('builtins.open', unittest.mock.mock_open()) as mock_file:
                result = backup.backup_database()
                self.assertEqual(result, "Successfully backed up test_db!")
                mock_file.assert_any_call(f'backup/test_db-mysql/{int(datetime.now().timestamp())}/table1.DDL.sql', 'w')
                mock_file.assert_any_call(f'backup/test_db-mysql/{int(datetime.now().timestamp())}/table1.DML.sql', 'w')
                mock_file.assert_any_call(f'backup/test_db-mysql/{int(datetime.now().timestamp())}/test_db.DCL.sql', 'w')

    @patch('os.makedirs')
    def test_backup_separate_postgresql(self, mock_makedirs):
        mock_makedirs.return_value = None

        backup = Backup(db_type="postgresql", database_name="test_db", connection_string="test_conn", is_save_multiple=True)
        mock_sql_dict = {
            "ddl": {"table1": "CREATE TABLE table1;"},
            "dml": {"table1": "INSERT INTO table1 VALUES (1);"},
            "dcl": {"test_db": "GRANT ALL PRIVILEGES;"}
        }
        with patch.object(backup, 'backup_separate', return_value=mock_sql_dict):
            with patch('builtins.open', unittest.mock.mock_open()) as mock_file:
                result = backup.backup_database()
                self.assertEqual(result, "Successfully backed up test_db!")
                mock_file.assert_any_call(f'backup/test_db-postgresql/{int(datetime.now().timestamp())}/table1.DDL.sql', 'w')
                mock_file.assert_any_call(f'backup/test_db-postgresql/{int(datetime.now().timestamp())}/table1.DML.sql', 'w')
                mock_file.assert_any_call(f'backup/test_db-postgresql/{int(datetime.now().timestamp())}/test_db.DCL.sql', 'w')


if __name__ == '__main__':
    unittest.main()
