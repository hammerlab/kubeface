from numpy import testing
import sys

from kubeface import (
	broadcast,
	serialization)


def test_sanity():
	big = "x" * 1000000
	big_wrapped = broadcast.Broadcast('/tmp', 'test', big)
	ser = serialization.dumps(big_wrapped)
	testing.assert_(sys.getsizeof(big) > 1000000)
	testing.assert_(sys.getsizeof(ser) < 500)  # this should just be the path, so should be tiny

	# sanity checK: make sure that deserializing gets the data back
	unser = serialization.loads(ser)
	testing.assert_equal(big, unser.data)
