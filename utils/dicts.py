from inspect import getmembers, isroutine

def class_to_dict(obj):
    attrs = getmembers(obj), lambda a:not(isroutine(a))
    attrs = [a for a in attrs[0] if not(a[0].startswith('__') and a[0].endswith('__'))]
    return dict(attrs)