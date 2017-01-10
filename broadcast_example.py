"""
Kubeface example with broadcast variables.

Prepends numbers 1-3 to a big string, showing how to use broadcast variables to reduce the
size of the uploaded task.

Example:

$ python broadcast_example.py --backend local-process --storage-prefix /tmp

"""

import argparse
from collections import Counter
import logging
import sys

import kubeface

parser = argparse.ArgumentParser(usage=__doc__)
kubeface.Client.add_args(parser)  # Add kubeface arguments


def main(argv):
	args = parser.parse_args(argv)
	logging.basicConfig(
        format="%(asctime)s.%(msecs)d %(levelname)s %(module)s - %(funcName)s:"
        " %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stderr,
        level=logging.INFO)

	client = kubeface.Client.from_args(args)
	input_values = range(3)

	big_string = "i am a string" * 100000
	big_wrapped = client.broadcast(big_string)

	logging.info('Using broadcast variable: note size of uploaded task')
	def my_func_with_broadcast(x):
		data = big_wrapped.value()
		return str(x) + data
	results = client.map(my_func_with_broadcast, input_values)
	for (x, result) in zip(input_values, results):
		print("%d, %s" % (x, Counter(result)))

	logging.info('Now running without broadcast variable: see uploaded task size again')
	def my_func_without_broadcast(x):
		return str(x) + big_string
	results = client.map(my_func_without_broadcast, input_values)
	for (x, result) in zip(input_values, results):
		print("%d, %s" % (x, Counter(result)))


if __name__ == '__main__':
    main(sys.argv[1:])