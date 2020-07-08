# pylint: disable=import-error
import os
import yaml
import json


class ConfigManager:
    @classmethod
    def get_config(cls, config_file=None):
        config_file = config_file or os.environ["DEFAULT_CONFIG"]
        config = cls.__load_config(config_file)
        return config

    @classmethod
    def get_config_value(cls, component, value=None, config_file=None):
        config = cls.get_config(config_file)
        return config[component][value] if value is not None else config[component]

    @staticmethod
    def __load_config(filename):
        try:
            with open(filename) as f:
                config = yaml.load(f, yaml.SafeLoader)
            config = replace_env(config)
        except Exception as e:
            raise Exception("Error: Can't parse config file {}. {}".format(filename, str(e)))
        return config


def replace_env(item):
    if isinstance(item, str):
        if var := find_env(item):
            item = replace_var(item, var)
        return item
    if isinstance(item, (list, tuple)):
        return [replace_env(i) for i in item]
    if isinstance(item, dict):
        keys = replace_env(list(item.keys()))
        vals = replace_env(list(item.values()))
        return dict(zip(keys, vals))
    return item


def replace_var(item, var):
    prefix, suffix = ("${", "}") if var.startswith("${") else ("$", "")
    var_name = var.replace(prefix, "").replace(suffix, "")
    return item.replace(var, os.environ[var_name])


def find_env(s):
    start = s.find("$")
    if start < 0:
        return None
    s = s[start:]
    for c in ["}", " ", ",", "="]:
        end = s.find(c)
        if end >= 0:
            return s[: end + 1]
    return s
