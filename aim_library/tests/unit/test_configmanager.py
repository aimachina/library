import os

import pytest
from utils.configmanager import replace_env


@pytest.fixture
def environment():
    os.environ["TEST_EMPTY"] = ""
    os.environ["TEST_STR"] = "some string"


def test_replace_var_in_str(environment):
    config = "some_value=${TEST_STR}"
    config = replace_env(config)
    assert config == "some_value=some string"


def test_replace_vars_in_list():
    config = ["${TEST_EMPTY}", "${TEST_STR}"]
    config = replace_env(config)
    assert config == ["", "some string"]


def test_replace_vars_in_dict_keys():
    config = {"${TEST_EMPTY}": "", "${TEST_STR}": "some string"}

    config = replace_env(config)
    assert list(config.keys()) == list(config.values())


def test_replace_vars_in_dict_vals():
    config = {
        "": "${TEST_EMPTY}",
        "some string": "${TEST_STR}",
    }

    config = replace_env(config)
    assert list(config.keys()) == list(config.values())


def test_replace_vars_in_list_of_dicts():
    config = {"list": [{"": "${TEST_EMPTY}"}, {"some string": "${TEST_STR}"},]}
    config = replace_env(config)
    assert config == {"list": [{"": ""}, {"some string": "some string"},]}


def test_load_and_parse_valid_yaml():
    import yaml

    with open("tests/fixtures/test_config.yml") as f:
        config = yaml.load(f, yaml.SafeLoader)

    config = replace_env(config)
    assert "$" not in str(config)


def test_ints_and_floats_are_preserved():
    config = {"some int": 0, "some float": 1.0}
    new_config = replace_env(config)
    assert config == new_config
