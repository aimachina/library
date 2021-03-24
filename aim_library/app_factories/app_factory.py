# pylint: disable=import-error
import logging
from importlib import import_module
import uuid

from flask import Flask, jsonify
from flask_cors import CORS

# pylint: disable=no-name-in-module
from app_factories.api_factory import create_api
from aim_library.utils.configmanager import ConfigManager
from aim_library.utils.common import make_url

# Enable for serving over https
from werkzeug.contrib.fixers import ProxyFix

logging.getLogger("flask_cors").level = logging.DEBUG


def make_flask():
    flask_app = Flask(__name__)
    flask_app.config["SECRET_KEY"] = str(uuid.uuid4())
    flask_app.config["MAX_CONTENT_LENGTH"] = 8 * 1024 * 1024
    flask_app.url_map.strict_slashes = False
    flask_app.wsgi_app = ProxyFix(flask_app.wsgi_app)

    if "database" in ConfigManager.get_config():
        config_database(flask_app, ConfigManager.get_config_value("database"))

    apis_config = ConfigManager.get_config_value("apis")

    for api_config in apis_config.values():
        api = create_api(api_config)
        api.init_app(flask_app)

    # pylint: disable=unused-variable
    @flask_app.errorhandler(422)
    def handle_error(err):
        headers = err.data.get("headers", None)
        messages = err.data.get("messages", ["Invalid request."])
        if headers:
            return jsonify({"errors": messages}), err.code, headers
        return jsonify({"errors": messages}), err.code

    CORS(flask_app)
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
