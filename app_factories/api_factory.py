# pylint: disable=import-error
from importlib import import_module

# from flask_restplus import Api
from flask_restx import Api

# pylint: disable=no-name-in-module
from utils.configmanager import ConfigManager
from app import resources

config = ConfigManager.get_config_value('ory') 
OAUTH2_HOST = config['oauth2_host']

authorizations = {
    'apikey': {
        'type': 'apiKey',
        'in': 'header',
        'name': 'Authorization'
    },
    'oauth2': {
        'type': 'oauth2',
        'flow': 'accessCode',
        'tokenUrl': f'{OAUTH2_HOST}/oauth2/token',
        'authorizationUrl': f'{OAUTH2_HOST}/oauth2/auth',
        'scopes': {
            'openid': 'Request token_id',
            'offline': 'Request refresh_token',
        }
    }
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
