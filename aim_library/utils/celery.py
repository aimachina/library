# pylint: disable=import-error
from celery import Celery
from utils.common import make_url
from utils.configmanager import ConfigManager


class CeleryManager:

    __instance = None
    __flask_app = None

    @classmethod
    def get_instance(cls, use_flask=True, flask_app=None):
        if not cls.__instance:
            name = flask_app.import_name if flask_app else __name__
            broker_url = make_url(ConfigManager.get_config_value("celery", "broker"))
            results_backend_url = make_url(ConfigManager.get_config_value("celery", "results_backend"))
            celery = Celery(name, broker=broker_url, backend=results_backend_url,)

            if use_flask:
                if use_flask and not cls.__flask_app:
                    from app.app_factory import make_flask

                    flask_app = flask_app or make_flask()
                    flask_app.config.update(
                        CELERY_BROKER_URL=broker_url, CELERY_RESULT_BACKEND=results_backend_url,
                    )
                    cls.__flask_app = flask_app
                celery.conf.update(cls.__flask_app.config)

                class ContextTask(celery.Task):
                    def __call__(self, *args, **kwargs):
                        with flask_app.app_context():
                            return self.run(*args, **kwargs)

                celery.Task = ContextTask
            cls.__instance = celery
        return cls.__instance
