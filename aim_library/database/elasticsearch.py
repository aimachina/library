# pylint: disable=import-error
# pylint: disable=no-name-in-module
from elasticsearch import Elasticsearch

from aim_library.utils.configmanager import ConfigManager


def make_es(retries=30, config=None):
    config = config or ConfigManager.get_config_value("database", "elasticsearch")
    while retries != 0:
        try:
            if config['ochestrator'] == 'kubernetes':
                conf = config['kubernetes']
                es = Elasticsearch(
                    conf["hosts"],
                    scheme="https",
                    verify_certs=False,
                    timeout=30,
                    max_retries=10,
                    retry_on_timeout=True,
                    http_auth=(conf['user'], conf['secret'])
                    )
            else:
                es = Elasticsearch(config['compose']["hosts"])
            init_es(es)
            return es
        except:
            import time

            time.sleep(5)
            print(f"Elasticsearch is not ready... Retrying in 5s (retries: {retries})")
            retries -= 1


def init_es(es, indices=None):
    indices = indices or ConfigManager.get_config_value("search", "indices")
    for index in indices:
        if not es.indices.exists(index):
            es.indices.create(index)


def lazy_es():
    def factory():
        if not factory.es:
            print("Elasticsearch is online!")
            factory.es = make_es()
        return factory.es

    factory.es = None
    return factory
