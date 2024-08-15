import os
import logging
class Backup:

    def __init__(self, database_name: str, connection_string: str, table_name: str = None, type: str = None,
                 is_save_one: bool = False, is_save_multiple: bool = False, save_into: str = None) -> None:
        logger = logging.getLogger(__name__)

        self.database_name = database_name
        self.connection_string = connection_string
        self.table_name = table_name
        self.type = type
        self.is_save_one = is_save_one
        self.is_save_multiple = is_save_multiple
        self.save_into = save_into
        if save_into is not None:
            if os.path.isdir(save_into):
                os.makedirs(save_into, exist_ok=True)
            else:
                logger.error('Provided path is not dir or does\'t exist!')


