from abc import ABC, abstractmethod


class Database(ABC):

    @abstractmethod
    def __init__(self, database_name: str, connection_string: str) -> None:
        pass

    @abstractmethod
    def get_database_structure(self) -> str:
        """Get database structure"""
        pass

    @abstractmethod
    def get_database_data(self) -> dict:
        """Get database data"""
        pass

    @abstractmethod
    def get_table(self) -> dict:
        """Get table structure with data"""
        pass

    @abstractmethod
    def restore_database_structure(self, database_data) -> bool:
        """Restore database structure"""
        pass

    @abstractmethod
    def restore_database_data(self, database_data) -> bool:
        """Restore database data"""
        pass

    @abstractmethod
    def restore_table(self, table_data) -> bool:
        """Restore database data"""
        pass
