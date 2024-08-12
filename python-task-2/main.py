import os
import logging
import argparse

from dotenv import load_dotenv

from src.db import Database
from src.backup_db import BackupDatabase

load_dotenv()

parser = argparse.ArgumentParser(description='Database Backup and Restore utility')
subparsers = parser.add_subparsers(dest='command', help='commands')
backup_parser = subparsers.add_parser('backup', help='Backup database')

os.makedirs('backups', exist_ok=True)

backup_parser.add_argument(
    '--db',
    required=True,
    help='Database name',
    action='store'
)
backup_parser.add_argument(
    '--table',
    help='Table to backup.',
    action='store'
)
backup_parser.add_argument(
    '--type',
    choices=['structure', 'data', 'full'],
    help='Type of database backup'
)

backup_parser.add_argument(
    "--save-one",
    help="Save all in one",
    action="store_true"
)
backup_parser.add_argument(
    "--save-multi",
    help="Save in multiple files.",
    action="store_true"
)
backup_parser.add_argument(
    "--save-into",
    help="Specify in which folder save backup.",
)

restore_parser = subparsers.add_parser('restore', help='Restore database')
restore_parser.add_argument(
    '--file',
    help='File to restore.',
    required=True
)
restore_parser.add_argument(
    '--type',
    choices=['structure', 'data', 'full'],
    help='Type of database restoration.',
    action='store'
)
restore_parser.add_argument(
    '--table',
    help='Table to restore.',
    action='store'
)
parser.add_argument(
    '-v', '--verbose',
    action='count',
    help='verbose mode'
)

args = parser.parse_args()

if args.type == 'structure':
    backup = BackupDatabase(
        database_url=f'mysql+mysqlconnector://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{args.db}',
        is_structure=True,
        database_name=args.db)
    os.makedirs(f'backups/{args.db}', exist_ok=True)
    backup.backup()
