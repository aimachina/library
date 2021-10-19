# pylint: disable=import-error
import os
from http.client import responses
import uuid
from functools import wraps
from typing import Callable


def make_url(db_config, include_db=True):
    url = db_config["type"]
    url += "://" + db_config["user"]
    url += ":" + db_config["password"]
    url += "@" + db_config["host"]
    url += ":" + db_config["port"]
    if include_db:
        url += "/" + db_config["db"]
    return url


def make_jsend_response(code=200, status="success", data=None, message=None):
    if message is None:
        message = responses[code]
    response = {}
    if 400 <= code < 500:
        status = "fail"
    elif code >= 500:
        status = "error"
    if data is not None:
        response["data"] = _make_response(data)
    response["status"] = status
    response["message"] = message
    return response, code


def _make_response(data):
    resp = {}
    if isinstance(data, list):
        resp["count"] = len(data)
        resp["values"] = data
    else:
        resp = data
    return resp


def uuid_factory(name: str) -> Callable[[], str]:
    prefix = name + "-"
    return lambda: prefix + str(uuid.uuid4())


def extract_attr(item, attr_name):
    attr_names = attr_name.split(".")
    for name in attr_names:
        if type(item) is dict:
            item = item[name] if name in item else None
        else:
            item = getattr(item, name, None)
        if not item:
            return None
    return item


def flatten(lists):
    lst = []
    if type(lists) is not str:
        for l in lists:
            lst += l
        return lst
    return lists


def make_path(*items):
    return "/".join(items)


def log_call(f):
    @wraps(f)
    def _f(*args, **kwargs):
        a = [type(a) for a in args]
        k = {k: type(v) for k, v in kwargs.items()}
        print(f"{_f.__name__} called with\nargs: {a}\nand kwargs: {k}")
        return f(*args, **kwargs)

    return _f


def parse_params(params):
    from flask_restplus import reqparse

    parser = reqparse.RequestParser()
    for param in params:
        parser.add_argument(**param)
    parsed_params = parser.parse_args()
    parsed_params = {k: v for k, v in parsed_params.items() if v is not None}
    return parsed_params


def batch_generator(generator, batch_size=16):
    while True:
        try:
            batch = []
            for _ in range(batch_size):
                batch.append(next(generator))
            yield batch
        except StopIteration:
            yield batch
            break


def enabled_by_env(var, truthy_vals=("1", 1, True, "true", "enabled")):
    value = os.getenv(var, "False")
    value = value.lower() if isinstance(value, str) else value
    return value in truthy_vals
