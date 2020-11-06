import os

import yaml
from typing import Any, Optional


class ConfigManager:
    @classmethod
    def get_config(cls, config_file: str = None) -> dict:
        config_file = config_file or os.environ["DEFAULT_CONFIG"]
        config = cls.__load_config(config_file)
        return config

    @classmethod
    def get_config_value(cls, component: str, value: str = None, config_file: str = None) -> Any:
        config = cls.get_config(config_file)
        return config[component][value] if value is not None else config[component]

    @staticmethod
    def __load_config(filename: str) -> dict:
        try:
            with open(filename) as file:
                config = yaml.load(file, yaml.SafeLoader)
            config = replace_env(config)
        except Exception as ex:
            raise Exception("Error: Can't parse config file {}. {}".format(filename, str(ex))) from ex
        return config


def replace_env(item: Any) -> Any:
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


def replace_var(item: str, var: str) -> str:
    prefix, suffix = ("${", "}") if var.startswith("${") else ("$", "")
    var_name = var.replace(prefix, "").replace(suffix, "")
    return item.replace(var, os.environ[var_name])


def find_env(item: str) -> Optional[str]:
    start = item.find("$")
    if start < 0:
        return None
    item = item[start:]
    for char in ["}", " ", ",", "="]:
        end = item.find(char)
        if end >= 0:
            return item[: end + 1]
    return item
