"""
Kubeface simple example.

Computes the square of numbers 1 .. N, where N is specified on the commandline.

Example:

$ python more_complex_example.py --backend local-process --storage-prefix /tmp

"""

import argparse
import logging
import numpy
import sys

import kubeface

parser = argparse.ArgumentParser(usage=__doc__)
kubeface.Client.add_args(parser)  # Add kubeface arguments


def main_with_broadcast_variable(argv):
	args = parser.parse_args(argv)
	logging.basicConfig(
        format="%(asctime)s.%(msecs)d %(levelname)s %(module)s - %(funcName)s:"
        " %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stderr,
        level=logging.INFO)

	client = kubeface.Client.from_args(args)
	input_values = range(3)

	big_string = "i'm a string" * 1000000
	big_wrapped = client.broadcast(big_string)

	def my_func(x):
		data = big_wrapped.value()
		return str(x) + data

	results = client.map(my_func, input_values)

	for (x, result) in zip(input_values, results):
		print("%d, %d" % (x, len(result)))


def main_without_broadcast_variable(argv):
	args = parser.parse_args(argv)
	logging.basicConfig(
        format="%(asctime)s.%(msecs)d %(levelname)s %(module)s - %(funcName)s:"
        " %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stderr,
        level=logging.INFO)

	client = kubeface.Client.from_args(args)
	input_values = range(3)

	big_string = "i'm a string" * 1000000
	def my_func(x):
		return str(x) + big_string

	results = client.map(my_func, input_values)

	for (x, result) in zip(input_values, results):
		print("%d, %d" % (x, len(result)))


if __name__ == '__main__':
    main_with_broadcast_variable(sys.argv[1:])