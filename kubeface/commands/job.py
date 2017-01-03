'''
Get info on and manipulate jobs.
'''

import sys
import argparse
import collections
import logging

from ..client import Client
from ..common import configure_logging
from .. import naming

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("jobs", nargs="*")
parser.add_argument(
    "--cleanup",
    action="store_true",
    default=False)
parser.add_argument(
    "--include-done",
    action="store_true",
    default=False)

Client.add_args(parser)


parser.add_argument(
    "--quiet",
    action="store_true",
    default=False,
    help="")

parser.add_argument(
    "--verbose",
    action="store_true",
    default=False,
    help="")


def run(argv=sys.argv[1:]):
    args = parser.parse_args(argv)
    configure_logging(args)

    client = Client.from_args(args)
    summary = client.job_summary(
        job_names=args.jobs if args.jobs else None,
        include_done=args.include_done)

    if not summary:
        print("No jobs.")

    jobs_by_cache_key = collections.defaultdict(list)
    job_info_by_name = {}
    for job_info_tuple in summary:
        job_info = job_info_tuple._asdict()
        job_name = job_info.pop('job_name')
        del job_info['format']
        cache_key = naming.JOB.make_tuple(job_name).cache_key
        jobs_by_cache_key[cache_key].append(job_name)
        if job_name in job_info_by_name:
            logging.warning("Multiple status pages for job: %s: %s %s" % (
                job_name,
                job_info['job_status_page_name'],
                job_info_by_name[job_name]['job_status_page_name']))
        job_info_by_name[job_name] = job_info

    for cache_key in jobs_by_cache_key:
        print("Cache key: %s" % cache_key)
        for job_name in jobs_by_cache_key[cache_key]:
            info = job_info_by_name[job_name]
            print("\t%7s : %s" % (info['status'], job_name))
        print("")
