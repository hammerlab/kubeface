
class Task(object):
    def __init__(self, name, function, args, kwargs):
        self.name = name
        self.function = function
        self.args = args
        self.kwargs = kwargs

    def result_name(self):
        return "result-%s" % self.name

    def run(self):
        return self.function(*self.args, **self.kwargs)
