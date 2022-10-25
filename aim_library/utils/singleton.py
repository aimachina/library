class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if args:
            raise Exception('Avoid using positional args, use kwargs instead')

        if not cls in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)

        instance  = cls._instances[cls]
        for k,v in kwargs.items():
            setattr(instance, k, v)
        return instance
