# pylint: disable=import-error
# pylint: disable=no-name-in-module
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError

from aim_library.utils.configmanager import ConfigManager
from aim_library.utils.common import make_url

def make_db(db_config=ConfigManager.get_config_value("database", "mongo")):
    use_tls = db_config.get('use_tls', '').lower() == 'true'
    retry_writes = db_config.get('retry_writes', '').lower() == 'true'
    return MongoClient(make_url(db_config, include_db=False, use_tls=use_tls), connect=False, retryWrites=retry_writes)[db_config["db"]]

class DataBaseException(Exception):
    pass

class MongoClientSingleton:
    _db_connection = None

    @classmethod
    def get_db_connection(cls, db_config=ConfigManager.get_config_value("database", "mongo")):
        if cls._db_connection is None:
            cls._db_connection = make_db(db_config)
            print('Checking database ...', end='\r')
            try:
                if cls._db_connection.command('ping') != {'ok': 1.0}:
                    print('Checking database ... Failed - Cannot see database')
                    raise DataBaseException('Cannot see database')
                print('Checking database ... Done')
            except ServerSelectionTimeoutError as sste:
                print('Checking database ... Failed - Timeout error')
                raise sste
        return cls._db_connection