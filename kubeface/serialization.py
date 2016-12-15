import logging

import dill
import dill.detect

def check(obj):
    try:
        dill.loads(dill.dumps(obj))
    except Exception as e:
        logging.error(
                "Couldn't serialize: %s\n'%s'\nBad objects:\n%s" % (
                    str(obj), str(e), dill.detect.badobjects(obj, depth=2)))
        raise

def dumps(obj):
    check(obj)
    return dill.dumps(obj)


def dump(obj, fd):
    check(obj)
    return dill.dump(obj, fd)


def loads(s):
    return dill.loads(s)


def load(fd):
    return dill.load(fd)
