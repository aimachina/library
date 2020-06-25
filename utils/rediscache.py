import pickle
import redis
import copy
import builtins
import json

from utils.configmanager import ConfigManager


class RedisCache:
    __client = None
    __types_mapping = None

    @classmethod
    def init_client(cls, config=ConfigManager.get_config_value("cache", "redis")):
        cls.__client = cls.__client = redis.StrictRedis(
            host=config["host"], port=config["port"], db=config["db"], password=config["password"]
        )
        cls.__types_mapping = cls.__name__ + "__types_mapping"

    @classmethod
    def __get_types_mapping(cls, name):
        r = cls.get_client()
        types_mapping = r.hget(name, cls.__types_mapping) or {}
        if types_mapping:
            types_mapping = pickle.loads(types_mapping)
        return types_mapping

    @classmethod
    def get_client(cls):
        if not cls.__client:
            cls.init_client()
        return cls.__client

    def set_mapping(self, name, mapping):
        r = self.get_client()
        types_mapping = self.__get_types_mapping(name)
        serialized = self.serialize_mapping(mapping, types_mapping)
        result = r.hmset(name, serialized)
        return result

    def get_mapping(self, name):
        r = self.get_client()
        mapping = r.hgetall(name)
        mapping = {k.decode("utf-8"): v for k, v in mapping.items()}
        if mapping:
            mapping = self.deserialize_mapping(mapping, deep_copy=False)
        return mapping

    def set_value(self, name, key, value):
        return self.set_mapping(name, {key: value})

    def get_value(self, name, key):
        values = self.get_values(name, [key])
        return values[0]

    def get_values(self, name, keys):
        r = self.get_client()
        values = r.hmget(name, *keys, self.__types_mapping)
        keys.append(self.__types_mapping)
        mapping = {k: v for k, v in zip(keys, values)}
        if mapping:
            mapping = self.deserialize_mapping(mapping, deep_copy=False)
        return list(mapping.values())

    def __clean_types_mapping(self, name, keys):
        r = self.get_client()
        types_mapping = self.__get_types_mapping(name)
        if types_mapping:
            [types_mapping.pop(k) for k in keys if k in types_mapping]
            r.hset(name, self.__types_mapping, pickle.dumps(types_mapping))

    def delete_values(self, name, keys):
        r = self.get_client()
        self.__clean_types_mapping(name, keys)
        return r.hdel(name, *keys)

    def delete_value(self, name, key):
        r = self.get_client()
        self.__clean_types_mapping(name, [key])
        return r.hdel(name, key)

    def delete_keys(self, names):
        r = self.get_client()
        return r.delete(*names)

    def delete_key(self, name):
        return self.delete_keys([name])

    def clear(self):
        r = self.get_client()
        return r.flushdb()

    def exists(self, name):
        r = self.get_client()
        return r.exists(name)

    def value_exists(self, name, key):
        r = self.get_client()
        return r.hexists(name, key)

    def get_all_keys_in_mapping(self, name):
        r = self.get_client()
        keys = [k.decode("utf-8") for k in r.hkeys(name)]
        keys.remove(self.__types_mapping)
        return keys

    @classmethod
    def serialize_mapping(cls, mapping, types_mapping):
        serialized = {}
        for k, v in mapping.items():
            if should_pickle(v):
                serialized[k] = pickle.dumps(v)
                types_mapping[k] = "pickled"
            else:
                serialized[k] = v
                types_mapping[k] = type(v).__name__
        serialized[cls.__types_mapping] = pickle.dumps(types_mapping)
        return serialized

    @classmethod
    def deserialize_mapping(cls, mapping, deep_copy=True):
        types_mapping = pickle.loads(mapping.pop(cls.__types_mapping))
        deserialized = copy.deepcopy(mapping) if deep_copy else mapping.copy()
        for k in deserialized:
            if k in types_mapping:
                deserialized[k] = coerce_obj(deserialized[k], types_mapping[k])
        return deserialized

    def expire(self, name, seconds=120):
        r = self.get_client()

        r.expire(name, seconds)

    def expireat(self, name, timestamp):

        r = self.get_client()
        r.expireat(name, timestamp)


ignore_coercion = [bytes.__name__]


def coerce_obj(obj, type_name):
    if type_name in ignore_coercion:
        return obj
    if type_name == "pickled":
        return pickle.loads(obj)
    _type = getattr(builtins, type_name)
    if _type is str:
        return obj.decode("utf-8")
    return _type(obj)


cache_types = [str, int, bytes, float]
should_pickle = lambda x: type(x) not in cache_types


def redis_cachable(r, name, timeout=120):
    def _set_name(f):
        def _redis_cachable(key, *args, **kwargs):
            key_name = name + "-" + key
            if r.exists(key_name):
                result = r.get(key_name)
            else:
                result = f(key, *args, **kwargs)
                r.set(key_name, result, ex=timeout)
            return result

        return _redis_cachable

    return _set_name


def invalidate_key(r, name):
    def _set_name(f):
        def _invalidate_key(key, *args, **kwargs):
            key_name = name + "-" + key
            r.delete(key_name)
            return f(key, *args, **kwargs)

        return _invalidate_key

    return _set_name


def replace_key(r, name, timeout=30):
    def _set_name(f):
        def _replace_key(key, data, *args, **kwargs):
            key_name = name + "-" + key
            if r.exists(key_name):
                r.delete(key_name)
            f(key, data, *args, **kwargs)
            r.set(key_name, data, ex=timeout)

        return _replace_key

    return _set_name


def make_redis(redis_config):
    return redis.StrictRedis(
        host=redis_config["host"],
        port=redis_config["port"],
        db=redis_config["db"],
        password=redis_config["password"],
    )
