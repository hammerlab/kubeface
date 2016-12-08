import dill


def dumps(obj):
    return dill.dumps(obj)


def dump(obj, fd):
    return dill.dump(obj, fd)


def loads(s):
    return dill.loads(s)


def load(fd):
    return dill.load(fd)
