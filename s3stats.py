#!/usr/bin/env python
# -*- coding: utf-8 -*- 

# **********
# Filename:         s3stats.py
# Description:      AWS S3 bucket statistics tool.
# Author:           Marc Vieira Cardinal
# Creation Date:    August 01, 2016
# Revision Date:    August 02, 2016
# **********


import re
import sys
import boto
import json
import time
import logging
from os import getenv, path
from fnmatch import fnmatch
from functools import partial
from hurry.filesize import size
from argparse import ArgumentParser
from prettytable import PrettyTable
from collections import defaultdict
from multiprocessing.dummy import Pool as ThreadPool


#############
# Logging Utilities
###

class Logging(object):
    """A logging utility class."""

    def __init__(self):
        self.log = logging.Logger(path.basename(__file__))
        self.log.stream = sys.stderr
        self.logHandler = logging.StreamHandler(self.log.stream)
        self.log.addHandler(self.logHandler)

    def Configure(self, args):
        """Configure the logger based on command-line arguments.

        Args:
            args (dict): The parsed command-line arguments.
        """
        DT_FORMAT = '%Y-%m-%d %H:%M:%S UTC'

        self.logHandler.setFormatter(logging.Formatter(
            '%(message)s',
            DT_FORMAT))
        if args.debug:
            self.logHandler.setFormatter(logging.Formatter(
                '  (%(levelname).1s)%(filename)s:%(lineno)-4d %(message)s',
                DT_FORMAT))
            self.log.setLevel(logging.DEBUG)
        elif args.verbose:
            self.log.setLevel(logging.INFO)
        else:
            self.log.setLevel(logging.ERROR)

    def GetLoggers(self):
        """Return a list of the logger methods: (debug, info, warn, error)."""
        return self.log.debug, self.log.info, self.log.warn, self.log.error


def LogCalls(func):
    """Decorator to log function calls for debugging."""
    def wrapper(*args, **kargs):
        callStr = "%s(%s)" % (func.__name__, ", ".join([repr(p) for p in args] + ["%s=%s" % (k, repr(v)) for (k, v) in list(kargs.items())]))
        debug(">> %s", callStr)
        ret = func(*args, **kargs)
        debug("<< %s: %s", callStr, repr(ret))
        return ret
    return wrapper


#############
# Varia
###

@LogCalls
def LifecycleToDict(lc):
    """Convert an s3 Lifecycle object to a dictionary.

    Args:
        lc (boto.s3.lifecycle.Lifecycle): The element to be converted to a dict.

    Returns:
        dict: The dictionary representation of the lifecycle object.
    """
    return { "prefix"    : lc.prefix,
             "status"    : lc.status,
             "expiration": { "date": lc.expiration.date,
                             "days": lc.expiration.days },
             "transition": lc.transition }


@LogCalls
def LoggingStatusToDict(loggingStatus):
    """Convert an s3 BucketLoging object to a dictionary.

    Args:
        loggingStatus (boto.s3.bucketlogging.BucketLogging): The element to be
            converted to a dict.

    Returns:
        dict: The dictionary representation of the logging object.
    """
    return { "target": loggingStatus.target,
             "prefix": loggingStatus.prefix,
             "grants": loggingStatus.grants }


@LogCalls
def Lister(bucket, versions = False):
    """Select an appropriate s3 bucket lister.

    Args:
        bucket (boto.s3.bucket): The bucket for which a lister is requested.
        versions (bool): True we want to list versions, False we don't.

    Returns:
        Either of boto.s3.bucket.list_versions or boto.s3.bucket.list.
    """
    return bucket.list_versions if versions else bucket.list


@LogCalls
def Stats(bucket, args):
    """Calculate the statistics of a given bucket based on command-line args.

    Args:
        bucket (boto.s3.bucket): The bucket object to gather stats for.
        args (dict): The result of command-line arguments parsing.

    Returns:
        dict: A dictionary representation of the statistics for a given bucket.
    """
    execTime = time.time()

    result = {
        "name"          : bucket.name,
        "creationDate"  : bucket.creation_date,
        "numberOfFiles" : 0,
        "sizeOfFiles"   : 0,
        "modifiedDate"  : "",
        "storageClasses": defaultdict(lambda: defaultdict(int)),
        "encrypted"     : defaultdict(lambda: defaultdict(int)),
    }

    if args.bucketDetails:
        try:
            result["lifecycle"] = dict([ (lc.id, LifecycleToDict(lc)) for lc in bucket.get_lifecycle_config() ])
        except:
            pass

        result["location"] = bucket.get_location()
        if result["location"] == "":
            result["location"] = "DEFAULT"

        result["logging"]  = LoggingStatusToDict(bucket.get_logging_status())
        result["tags"]     = dict([ (t.key, t.value) for t in bucket.get_tags()[0] ])
        result["version"]  = bucket.get_versioning_status()

    for key in Lister(bucket, args.sumPrevVersions)(prefix = args.prefix):
        if len(ApplyFilters(args.filter, [key], args.filterRe)) > 0:

            result["numberOfFiles"] += 1
            result["sizeOfFiles"] += key.size

            result["storageClasses"][key.storage_class]["numberOfFiles"] += 1
            result["storageClasses"][key.storage_class]["sizeOfFiles"] += key.size

            result["encrypted"][key.encrypted]["numberOfFiles"] += 1
            result["encrypted"][key.encrypted]["sizeOfFiles"] += key.size

            if key.last_modified > result["modifiedDate"]:
                result["modifiedDate"] = key.last_modified

    info("[{}] listed in {} seconds".format(bucket.name, time.time() - execTime))
    return result


@LogCalls
def ParallelStats(buckets, threads=8):
    """A util function that requests the bucket Stats in parallel.

    Args:
        buckets (list): A list of string, one item per bucket.
        threads (int): The number of threads in the ThreadPool.

    Returns:
        list: A list of dictionary based on the result of Stats().
    """
    pool = ThreadPool(threads)
    results = pool.map(partial(Stats, args=args), buckets)
    pool.close()
    pool.join()
    return results


@LogCalls
def ApplyFilters(pattern, items, regex):
    """Filter a given list based on a glob or regex pattern.

    Args:
        pattern (string): The pattern on which to filter, glob or regex.
        items (list): List of string to filter.
        regex (bool): True the pattern is a regex, False it is a glob.

    Returns:
        list: The filtered list of items.
    """
    if regex:
        return [ b for b in items if re.match(pattern, b.name) ]

    return [ b for b in items if fnmatch(b.name, pattern) ]


@LogCalls
def PrintResults(args, results):
    """Prints a given results object based on command-line arguments.

    Args:
        args (dict): Parsed command-line arguments.
        results (list): A list of dictionaries representing the result of
            Stats() for each bucket.
    """
    # Sum for the total row
    numberOfFiles = sum([ r["numberOfFiles"] for r in results ])
    sizeOfFiles = sum([ r["sizeOfFiles"] for r in results ])

    if args.byRegion:
        regions = set([ r["location"] for r in results ])

        t = PrettyTable(["region", "numberOfFiles", "sizeOfFiles", "% size"])

        t.align["region"] , = "l"
        t.align["numberOfFiles"], t.align["sizeOfFiles"], t.align["% size"] = "r", "r", "r"

        for region in regions:
            regionNumberOfFiles = sum([ r["numberOfFiles"] for r in results if r["location"] == region ])
            regionSizeOfFiles = sum([ r["sizeOfFiles"] for r in results if r["location"] == region ])
            
            t.add_row([ region,
                        regionNumberOfFiles,
                        size(regionSizeOfFiles) if args.human else regionSizeOfFiles,
                        "{:,.2f}%".format(float(regionSizeOfFiles) / float(sizeOfFiles) * 100) ])

        t.add_row([ "Total",
                    numberOfFiles,
                    size(sizeOfFiles) if args.human else sizeOfFiles,
                    "100.00%" ])
        print t

    elif args.byStorageType:
        t = PrettyTable(["region", "bucketName", "storageClass",
                         "numberOfFiles", "sizeOfFiles", "% size"])

        t.align["region"] , t.align["bucketName"] = "l", "l"
        t.align["numberOfFiles"], t.align["sizeOfFiles"], t.align["% size"] = "r", "r", "r"

        for result in results:
            for storClass in result["storageClasses"]:
                t.add_row([ result["location"],
                            result["name"],
                            storClass,
                            result["numberOfFiles"],
                            size(result["sizeOfFiles"]) if args.human else result["sizeOfFiles"],
                            "{:,.2f}%".format(float(result["sizeOfFiles"]) / float(sizeOfFiles) * 100) ])

        t.add_row([ "Total",
                    "",
                    "",
                    numberOfFiles,
                    size(sizeOfFiles) if args.human else sizeOfFiles,
                    "100.00%" ])
        print t

    elif args.byEncryption:
        t = PrettyTable(["region", "bucketName", "encryption",
                         "numberOfFiles", "sizeOfFiles", "% size"])

        t.align["region"] , t.align["bucketName"] = "l", "l"
        t.align["numberOfFiles"], t.align["sizeOfFiles"], t.align["% size"] = "r", "r", "r"

        for result in results:
            for enc in result["encrypted"]:
                t.add_row([ result["location"],
                            result["name"],
                            enc,
                            result["numberOfFiles"],
                            size(result["sizeOfFiles"]) if args.human else result["sizeOfFiles"],
                            "{:,.2f}%".format(float(result["sizeOfFiles"]) / float(sizeOfFiles) * 100) ])

        t.add_row([ "Total",
                    "",
                    "",
                    numberOfFiles,
                    size(sizeOfFiles) if args.human else sizeOfFiles,
                    "100.00%" ])
        print t

    else:
        t = PrettyTable(["region", "bucketName", "numberOfFiles", "sizeOfFiles", "% size"])

        t.align["region"] , t.align["bucketName"] = "l", "l"
        t.align["numberOfFiles"], t.align["sizeOfFiles"], t.align["% size"] = "r", "r", "r"

        for result in results:
            t.add_row([ result["location"],
                        result["name"],
                        result["numberOfFiles"],
                        size(result["sizeOfFiles"]) if args.human else result["sizeOfFiles"],
                        "{:,.2f}%".format(float(result["sizeOfFiles"]) / float(sizeOfFiles) * 100) ])

        t.add_row([ "Total",
                    "",
                    numberOfFiles,
                    size(sizeOfFiles) if args.human else sizeOfFiles,
                    "100.00%" ])
        print t


#############
# Main()
###

def Main(args):
    """Main, where the job dispatching happens and the result is printed.

    Args:
        args (dict): The result of the command-line argument parsing.
    """
    global debug, info, warn, error

    s3statsLogging = Logging()
    s3statsLogging.Configure(args)

    debug, info, warn, error = s3statsLogging.GetLoggers()

    if args.verbose:
        boto.set_stream_logger('boto')

    s3 = boto.connect_s3(args.awsKeyId, args.awsSecret)
    buckets = ApplyFilters(args.bucket, s3.get_all_buckets(), args.bucketRe)
    results = ParallelStats(buckets, args.threads)

    if args.format == "json":
        print json.dumps(results, sort_keys = True, indent = 4)
    else:
        PrintResults(args, results)


#############
# Main
###

if __name__ == "__main__":
    # Parse the command line arguments
    # Define the main parser (top-level)
    argParser = ArgumentParser(prog = path.basename(__file__))

    # Ways to specify credentials:
    # http://boto.cloudhackers.com/en/latest/boto_config_tut.html
    argParser.add_argument("--awsKeyId",
                           dest    = "awsKeyId",
                           action  = "store",
                           default = getenv("AWS_KEY_ID", None),
                           help    = "The access key id to use in S3"
                                     " operations, can be overridden by"
                                     " specifying the AWS_KEY_ID env variable.")
    argParser.add_argument("--awsSecret",
                           dest    = "awsSecret",
                           action  = "store",
                           default = getenv("AWS_SECRET", None),
                           help    = "The secret access key to use in S3"
                                     " operations, can be overridden by"
                                     " specifying the AWS_SECRET env variable.")
    argParser.add_argument("--threads",
                           dest    = "threads",
                           action  = "store",
                           type    = int,
                           default = 8,
                           help    = "The number of threads to use in"
                                     " multithreaded operations.")
    argParser.add_argument("--debug",
                           dest    = "debug",
                           action  = "store_true",
                           help    = "Enable the debug logging.")
    argParser.add_argument("--verbose",
                           dest    = "verbose",
                           action  = "store_true",
                           help    = "Enable the verbose logging.")
    argParser.add_argument("--format",
                           dest    = "format",
                           action  = "store",
                           choices = ["table", "json"],
                           default = "table",
                           help    = "Change the output format.")

    argParser.add_argument("--human-readable",
                           dest    = "human",
                           action  = "store_true",
                           help    = "Only when --format is table. Print sizes"
                                     " in human readable format (e.g., 1K 234M"
                                     " 2G).")
    argParser.add_argument("--by-region",
                           dest    = "byRegion",
                           action  = "store_true",
                           help    = "Group results by region.")
    argParser.add_argument("--by-storage-type",
                           dest    = "byStorageType",
                           action  = "store_true",
                           help    = "Split resulsts by storage type.")
    argParser.add_argument("--by-encryption",
                           dest    = "byEncryption",
                           action  = "store_true",
                           help    = "Split resulsts by encryption state.")
    argParser.add_argument("--bucket",
                           dest    = "bucket",
                           action  = "store",
                           default = "*",
                           help    = "Affected by --bucket-re, a regex or glob"
                                     " pattern used to filter bucket names.")
    argParser.add_argument("--bucket-re",
                           dest    = "bucketRe",
                           action  = "store_true",
                           help    = "Determines if --bucket should be"
                                     " interpreted as a regex, otherwise by"
                                     " glob.")
    argParser.add_argument("--filter",
                           dest    = "filter",
                           action  = "store",
                           default = "*",
                           help    = "Affected by --filter-re, a regex or glob"
                                     " pattern used to filter keys.")
    argParser.add_argument("--filter-re",
                           dest    = "filterRe",
                           action  = "store_true",
                           help    = "Determines if --filter should be"
                                     " interpreted as a regex, otherwise by"
                                     " glob.")
    argParser.add_argument("--prefix",
                           dest    = "prefix",
                           action  = "store",
                           default = "",
                           help    = "Only look at keys with this prefix when"
                                     "calculating the size and number of files.")
    argParser.add_argument("--sum-prev-versions",
                           dest    = "sumPrevVersions",
                           action  = "store_true",
                           help    = "Use all versions of a file in size and"
                                     "file count calculations.")
    argParser.add_argument("--bucket-details",
                           dest    = "bucketDetails",
                           action  = "store_true",
                           help    = "Retrieve additional bucket information in"
                                     " json mode.")

    args = argParser.parse_args()

    Main(args)
