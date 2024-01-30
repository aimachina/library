# pylint: disable=import-error
# pylint: disable=no-name-in-module
from pymongo import MongoClient

from aim_library.utils.configmanager import ConfigManager
from aim_library.utils.common import make_url


def make_db(db_config=ConfigManager.get_config_value("database", "mongo")):
    use_tls = db_config.get('use_tls', '').lower() == 'true'
    return MongoClient(make_url(db_config, include_db=False, use_tls=use_tls), connect=False)[db_config["db"]]

db = make_db()