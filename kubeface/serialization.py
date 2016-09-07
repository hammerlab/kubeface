import dill


def dumps(obj):
    return dill.dumps(obj)


def loads(s):
    return dill.loads(s)
