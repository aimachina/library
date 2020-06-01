# pylint: disable=import-error
# pylint: disable=no-name-in-module
import uuid
from importlib import import_module
from flask import Flask

from utils.configmanager import ConfigManager
from utils.common import make_url


def make_flask():
    flask_app = Flask(__name__)
    flask_app.config["SECRET_KEY"] = str(uuid.uuid4())
    flask_app.config["MAX_CONTENT_LENGTH"] = 8 * 1024 * 1024

    if "database" in ConfigManager.get_config():
        config_database(flask_app, ConfigManager.get_config_value("database"))

    return flask_app


sqlalchemy_backends = ["postgresql", "sqlite"]


def config_database(flask_app, config):
    module_name = "database." + next(iter(config.keys()))
    module = import_module(module_name)
    db = getattr(module, "db")
    db_config = getattr(module, "db_config")
    if db_config["type"] in sqlalchemy_backends:
        flask_app.config["SQLALCHEMY_DATABASE_URI"] = make_url(db_config)
        flask_app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = True
        db.init_app(flask_app)
