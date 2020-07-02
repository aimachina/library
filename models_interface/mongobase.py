# pylint: disable=import-error
# pylint: disable=no-name-in-module

from dataclasses import dataclass, field
import datetime
from marshmallow import Schema, fields, post_load, ValidationError

from database.mongo import db
from utils.common import uuid_factory


class BytesField(fields.Field):
    def _validate(self, value):
        if type(value) is not bytes:
            raise ValidationError("Invalid input type.")


CONSTRAINT_ERROR_MSG = "constraint_value can't be None when IBase.__force_constraint__ is True"


@dataclass
class IMongoBase:
    # pylint: disable=no-member
    # IBase
    _id: str
    created_at: datetime.datetime
    updated_at: datetime.datetime

    @classmethod
    def get_all(cls, constraint_value={}, skip=0, limit=0):
        if cls.__force_constraint__ and not constraint_value:
            raise ValueError(CONSTRAINT_ERROR_MSG)
        c = db[cls.__collection__]
        ser = cls.__serializer__
        rsp = c.find(constraint_value).skip(skip).limit(limit)
        return (ser.load(item) for item in rsp if item)

    @classmethod
    def get(cls, id_, constraint_value={}):
        filters = {"_id": id_}
        return cls.get_first(filters, constraint_value)

    @classmethod
    def get_many_by_ids(cls, ids, constraint_value={}):
        ids = set(ids)
        filters = [{"_id": id_} for id_ in ids]
        rsp = (cls.get_first(f, constraint_value) for f in filters)
        return (item for item in rsp if item)

    @classmethod
    def get_many_by(cls, filters, constraint_value={}):
        if cls.__force_constraint__ and not constraint_value:
            raise ValueError(CONSTRAINT_ERROR_MSG)
        c = db[cls.__collection__]
        ser = cls.__serializer__
        constrained_filter = filters.copy()
        constrained_filter.update(constraint_value)
        cursor = c.find(constrained_filter)
        rsp = (ser.load(obj) for obj in cursor if obj)
        return rsp

    @classmethod
    def get_first(cls, filters, constraint_value={}):
        if cls.__force_constraint__ and not constraint_value:
            raise ValueError(CONSTRAINT_ERROR_MSG)
        c = db[cls.__collection__]
        ser = cls.__serializer__
        constrained_filter = filters.copy()
        constrained_filter.update(constraint_value)
        d = c.find_one(constrained_filter)
        return ser.load(d) if d else None

    @classmethod
    def delete(cls, ids, constraint_value={}):
        if cls.__force_constraint__ and not constraint_value:
            raise ValueError(CONSTRAINT_ERROR_MSG)
        c = db[cls.__collection__]
        filters = [{"_id": id_} for id_ in ids]
        [f.update(constraint_value) for f in filters]
        [c.delete_one(f) for f in filters]

    def save(self, constraint_value={}):
        if self.__force_constraint__ and not constraint_value:
            raise ValueError(CONSTRAINT_ERROR_MSG)
        c = db[self.__collection__]
        serializer = self.__serializer__
        if not self.created_at:
            self.created_at = datetime.datetime.now()
        self.updated_at = self.created_at
        serialized = serializer.dump(self)
        serialized["created_at"] = self.created_at  # Bypass serialization for datetime fields
        serialized["updated_at"] = self.updated_at
        for field in constraint_value:
            if serialized[field] != constraint_value[field]:
                raise ValidationError(
                    "Can't match constraint value for IMongoBase instance {}".format(
                        list(constraint_value.keys())
                    )
                )

        c.insert_one(serialized)

    def update(self, data, constraint_value={}):
        if self.__force_constraint__ and not constraint_value:
            raise ValueError(CONSTRAINT_ERROR_MSG)
        serializer = self.__serializer__
        serialized = serializer.dump(self)
        for field in constraint_value:
            if serialized[field] != constraint_value[field]:
                raise ValidationError(
                    "Can't match constraint value for IMongoBase instance {}".format(
                        list(constraint_value.keys())
                    )
                )
        c = db[self.__collection__]
        c.update_one({"_id": self._id}, {"$set": data}, upsert=False)


class DatetimeField(fields.DateTime):
    def _deserialize(self, value, *args, **kwargs):
        if isinstance(value, datetime.datetime):
            value = value.isoformat()
        return super()._deserialize(value, *args, **kwargs)


class IMongoBaseSchema(Schema):
    # IBase
    _id = fields.String(load_only=True)
    created_at = DatetimeField(missing=datetime.datetime.now, format="iso")
    updated_at = DatetimeField(missing=datetime.datetime.now, format="iso")
