# -*- coding: utf-8 -*-
from __future__ import print_function
import aerospike
from aerospike import exception as e

try:
    from aerospike_helpers.operations import map_operations as mh
    from aerospike_helpers import cdt_ctx as ctxh
except:
    pass  # Needs Aerospike client >= 3.4.0
import calendar
import datetime
from optparse import OptionParser
import pprint
import random
import sys

# Options Parsing
usage = "usage: %prog [options]"
optparser = OptionParser(usage=usage, add_help_option=False)
optparser.add_option(
    "--help", dest="help", action="store_true", help="Displays this message."
)
optparser.add_option(
    "-U",
    "--username",
    dest="username",
    type="string",
    metavar="<USERNAME>",
    help="Username to connect to database.",
)
optparser.add_option(
    "-P",
    "--password",
    dest="password",
    type="string",
    metavar="<PASSWORD>",
    help="Password to connect to database.",
)
optparser.add_option(
    "-h",
    "--host",
    dest="host",
    type="string",
    default="127.0.0.1",
    metavar="<ADDRESS>",
    help="Address of Aerospike server.",
)
optparser.add_option(
    "-p",
    "--port",
    dest="port",
    type="int",
    default=3000,
    metavar="<PORT>",
    help="Port of the Aerospike server.",
)
optparser.add_option(
    "-n",
    "--namespace",
    dest="namespace",
    type="string",
    default="test",
    metavar="<NS>",
    help="Port of the Aerospike server.",
)
optparser.add_option(
    "-s",
    "--set",
    dest="set",
    type="string",
    default="nested_cdt_example",
    metavar="<SET>",
    help="Port of the Aerospike server.",
)
options, args = optparser.parse_args()
if options.help:
    optparser.print_help()
    print()
    sys.exit(1)


def version_tuple(version):
    return tuple(int(i) for i in version.split("."))


config = {"hosts": [(options.host, options.port)]}
try:
    client = aerospike.client(config).connect(options.username, options.password)
except e.ClientError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))
    sys.exit(2)

version = client.info_all("version")
release = list(version.values())[0][1].split(" ")[-1]
if version_tuple(aerospike.__version__) < version_tuple("3.9.0") or version_tuple(
    release
) < version_tuple("4.6.0.2"):
    print(
        "\nNested map and list operations were added in",
        "Aerospike database 4.6 / Python client 3.9.0.",
        "Please update to run this example.",
    )
    sys.exit(3)

if options.namespace and options.namespace != "None":
    namespace = options.namespace
else:
    namespace = None
set = options.set if options.set and options.set != "None" else None
key = (namespace, set, "pacman")
scores = {
    "CPU": [9800, {"dt": "2017-12-05 01:01:11", "ts": 1512435671573}],
    "ETC": [9200, {"dt": "2018-05-01 13:47:26", "ts": 1525182446891}],
    "SOS": [24700, {"dt": "2018-01-05 01:01:11", "ts": 1515114071923}],
    "ACE": [34500, {"dt": "1979-04-01 09:46:28", "ts": 291807988156}],
    "EIR": [18400, {"dt": "2018-03-18 18:44:12", "ts": 1521398652483}],
    "CFO": [17400, {"dt": "2017-11-19 15:22:38", "ts": 1511104958197}],
}

try:
    # create a KV-ordered map to contain the high scores of a game
    map_policy = {"map_order": aerospike.MAP_KEY_VALUE_ORDERED}
    ops = [mh.map_put_items("scores", scores, map_policy)]
    meta = {"ttl": aerospike.TTL_NEVER_EXPIRE}
    client.operate_ordered(key, ops, meta)
except e.RecordError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))
    sys.exit(4)

try:
    pp = pprint.PrettyPrinter(indent=2)
    print("\nGet all Pac-Man top scores, sorted by score (rank)")
    ops = [mh.map_get_by_rank_range("scores", 0, -1, aerospike.MAP_RETURN_KEY_VALUE)]
    k, m, b = client.operate(key, ops)
    by_rank = b["scores"]
    pp.pprint(by_rank)
    print("===============================================")

    # add an award icon to a specific player ("CFO") where awards have a type
    # and a count, some awards can be given once, and some more than once
    ctx = [
        ctxh.cdt_ctx_map_key("CFO"),
        # the attribute map is the second element of the tuple
        ctxh.cdt_ctx_list_index(1),
    ]
    ops = [
        # give the unicorn award exactly once
        mh.map_put(
            "scores", "awards", {"🦄": 1}, {
                "map_write_flags": aerospike.MAP_WRITE_FLAGS_CREATE_ONLY
                | aerospike.MAP_WRITE_FLAGS_NO_FAIL
            }, ctx)
    ]
    k, m, b = client.operate(key, ops)

    # assuming it's given once a day, grant the 'top score' award to the
    # current top score
    ctx = [
        ctxh.cdt_ctx_map_rank(-1),
        # the attribute map is the second element of the tuple
        ctxh.cdt_ctx_list_index(1),
    ]
    ctx2 = ctx + [ctxh.cdt_ctx_map_key("awards")]
    ops = [
        # create the top score award if it doesn't exist
        mh.map_put("scores", "awards", {"🏆": 0}, {
                "map_write_flags": aerospike.MAP_WRITE_FLAGS_CREATE_ONLY
                | aerospike.MAP_WRITE_FLAGS_NO_FAIL
            }, ctx),
        mh.map_increment("scores", "🏆", 1, ctx=ctx2),
    ]
    client.operate(key, ops)
    k, m, b = client.get(key)
    pp.pprint(b)
    print("===============================================")

    # grant the award once more
    client.operate(key, ops)

    # get the top three scores
    ops = [mh.map_get_by_rank_range("scores", -3, 3, aerospike.MAP_RETURN_KEY_VALUE)]
    k, m, b = client.operate(key, ops)
    pp.pprint(b["scores"])

except e.RecordError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))

client.close()
