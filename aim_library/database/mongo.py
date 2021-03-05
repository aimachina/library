# pylint: disable=import-error
# pylint: disable=no-name-in-module
from pymongo import MongoClient

from utils.configmanager import ConfigManager
from utils.common import make_url


def make_db(db_config=ConfigManager.get_config_value("database", "mongo")):
    return MongoClient(make_url(db_config, include_db=False), connect=False)[db_config["db"]]


db = make_db()
