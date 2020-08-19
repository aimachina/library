# pylint: disable=import-error
from importlib import import_module

# from flask_restplus import Api
from flask_restx import Api

# pylint: disable=no-name-in-module
from utils.configmanager import ConfigManager
from app import resources

authorizations = {
    'apikey': {
        'type': 'apiKey',
        'in': 'header',
        'name': 'X-API-KEY'
    },
}

def create_api(api_config):
    api = Api(
        prefix=api_config["prefix"],
        title=api_config["title"],
        version=api_config["version"],
        catch_all_404s=True,
        doc=api_config['doc_prefix'],
        authorizations=authorizations,
        security='apikey'
    )

    for module_name in api_config["resources"]:
        module = import_module("." + module_name, "app.resources")
        namespace = getattr(module, "api")
        api.add_namespace(namespace)

    return api
