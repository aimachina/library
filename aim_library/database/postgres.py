# pylint: disable=import-error
# pylint: disable=no-name-in-module
from flask_sqlalchemy import SQLAlchemy
from flask_marshmallow import Marshmallow

from aim_library.utils.configmanager import ConfigManager
from aim_library.utils.common import make_url


db_config = ConfigManager.get_config_value("database", "postgres")
db = SQLAlchemy()  # Initialize SQLAlchemy before Marshmallow
ma = Marshmallow()
