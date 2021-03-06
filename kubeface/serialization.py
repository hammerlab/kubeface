import logging

import dill
import dill.detect

PICKLE_PROTOCOL = 2

CHECK_SERIALIZATION = False


def check(obj):
    if not CHECK_SERIALIZATION:
        return
    try:
        dill.loads(dill.dumps(obj))
    except Exception as e:
        logging.error(
            "Couldn't serialize: %s\n'%s'\nBad objects:\n%s" % (
                str(obj), str(e), dill.detect.badobjects(obj, depth=2)))
        raise


def dumps(obj):
    check(obj)
    return dill.dumps(obj, protocol=PICKLE_PROTOCOL)


def dump(obj, fd):
    check(obj)
    return dill.dump(obj, fd, protocol=PICKLE_PROTOCOL)


def loads(s):
    return dill.loads(s)


def load(fd):
    return dill.load(fd)
