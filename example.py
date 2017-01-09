"""
Kubeface simple example.

Computes the square of numbers 1 .. N, where N is specified on the commandline.

Example:

$ python example.py 10 --backend local-process --storage-prefix /tmp

"""

import argparse
import sys

import kubeface

parser = argparse.ArgumentParser(usage=__doc__)
parser.add_argument("n", type=int)
kubeface.Client.add_args(parser)  # Add kubeface arguments

def my_function(x):
    return x**2


def main(argv):
    args = parser.parse_args(argv)
    client = kubeface.Client.from_args(args)

    input_values = range(1, args.n + 1)
    results = client.map(my_function, input_values) 

    for (x, result) in zip(input_values, results):
        print("%5d**2 = %5d" % (x, result))


if __name__ == '__main__':
    main(sys.argv[1:])


