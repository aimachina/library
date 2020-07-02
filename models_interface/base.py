# pylint: disable=import-error
# pylint: disable=no-name-in-module
from importlib import import_module
from sqlalchemy.ext.declarative import declarative_base

from utils.celery import CeleryManager
from utils.common import make_url
from utils.configmanager import ConfigManager

config = ConfigManager.get_config_value("database")
module_name = "database." + next(iter(config.keys()))
module = import_module(module_name)
db = getattr(module, "db")
ma = getattr(module, "ma")

CONSTRAINT_ERROR_MSG = "constraint_value can't be None when IBase.__force_constraint__ is True"


class IBase(db.Model):
    __abstract__ = True

    @classmethod
    def get_all(cls, constraint_value=None):
        if cls.__force_constraint__ and constraint_value is None:
            raise ValueError(CONSTRAINT_ERROR_MSG)

        if cls.__force_constraint__:
            return cls.query.filter_by(**constraint_value).all()
        else:
            return cls.query.all()

    @classmethod
    def get_many(cls, constraint_value=None, **params):
        if cls.__force_constraint__ and constraint_value is None:
            raise ValueError(CONSTRAINT_ERROR_MSG)
        if cls.__force_constraint__:
            query = cls.query.filter_by(**constraint_value)
        else:
            query = cls.query
        for k, v in params.items():
            if isinstance(v, list):
                query = query.filter(getattr(cls, k).in_(v))
            else:
                query = query.filter_by(**{k: v})
        return query.all()

    @classmethod
    def get_first(cls, constraint_value=None, **params):
        if cls.__force_constraint__ and constraint_value is None:
            raise ValueError(CONSTRAINT_ERROR_MSG)
        if cls.__force_constraint__:
            query = cls.query.filter_by(**constraint_value)
        else:
            query = cls.query
        query = cls.query
        return query.filter_by(**params).first()

    def save(self):
        db.session.add(self)
        db.session.commit()

    def update(self, data, constraint_value=None):
        if self.__force_constraint__ and constraint_value is None:
            raise ValueError(CONSTRAINT_ERROR_MSG)
        for k, v in data.items():
            if type(v) is dict:
                field = getattr(self, k)
                field.update(v)
            else:
                setattr(self, k, v)
        db.session.commit()

    @classmethod
    def delete(cls, ids, constraint_value=None):
        if cls.__force_constraint__ and constraint_value is None:
            raise ValueError(CONSTRAINT_ERROR_MSG)
        query = cls.__table__.delete().where(
            cls.id.in_(ids)
        )  # This pattern avoids querying first and then deleting
        db.session.execute(query)
        db.session.commit()
