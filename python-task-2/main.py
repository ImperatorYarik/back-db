import logging
import argparse

import src.backup as backup
import src.restore as restore


def backup_controller(args):
    bk = backup.Backup(db_type=args.db_type, database_name=args.db, connection_string=args.connection_string,
                       table_name=args.table, op_type=args.type, is_save_one=args.save_one,
                       is_save_multiple=args.save_multi, save_into=args.save_into)

    return bk.backup_database()


def restore_controller(args):
    rs = restore.Restore(db_type=args.db_type, database_name=args.db, connection_string=args.connection_string,
                         file=args.file, backup_version=args.backup_version, restore_type=args.type,
                         table_name=args.table)
    return rs.restore_database()


def main():
    parser = argparse.ArgumentParser(description='Database Backup and Restore utility')
    subparsers = parser.add_subparsers(dest='command', help='commands')
    backup_parser = subparsers.add_parser('backup', help='Backup database')

    backup_parser.add_argument(
        '--db',
        required=True,
        help='Database name',
        action='store'
    )
    backup_parser.add_argument(
        '--db-type',
        required=True,
        action='store'
    )
    backup_parser.add_argument(
        '--connection-string',
        required=True,
        action='store'
    )
    backup_parser.add_argument(
        '--table',
        help='Table to backup.',
        action='store',
        default=None
    )
    backup_parser.add_argument(
        '--type',
        choices=['structure', 'data'],
        default=None,
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
        '--db',
        required=True,
        help='Database name',
        action='store'
    )
    restore_parser.add_argument(
        '--connection-string',
        required=True,
        action='store'
    )
    restore_parser.add_argument(
        '--db-type',
        required=True,
        action='store'
    )
    restore_parser.add_argument(
        '--file',
        help='File to restore.',
    )
    restore_parser.add_argument(
        '--backup-version',
        help='Database backup version(timestamp)',
        action='store'
    )
    restore_parser.add_argument(
        '--type',
        choices=['structure', 'data'],
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

    if args.command == 'backup':
        print(backup_controller(args))
    if args.command == 'restore':
        print(restore_controller(args))


if __name__ == '__main__':
    main()
