# -*- coding: utf-8 -*-
from __future__ import print_function
import aerospike
from aerospike import exception as e
from aerospike_helpers.operations import list_operations as lh
from optparse import OptionParser
import pprint
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
    default="leaderboard",
    metavar="<SET>",
    help="Port of the Aerospike server.",
)
options, args = optparser.parse_args()
if options.help:
    optparser.print_help()
    print()
    sys.exit(1)

config = {"hosts": [(options.host, options.port)]}
try:
    client = aerospike.client(config).connect(options.username, options.password)
except e.ClientError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))
    sys.exit(2)

if options.namespace and options.namespace != "None":
    namespace = options.namespace
else:
    namespace = None
set = options.set if options.set and options.set != "None" else None
key = (namespace, set, 1)

wr_100m_1 = [
    [10.06, "Bob Hayes", "Tokyo, Japan", "October 15, 1964"],
    [10.03, "Jim Hines", "Sacramento, USA", "June 20, 1968"],
    [10.02, "Charles Greene", "Mexico City, Mexico", "October 13, 1968"],
    [9.95, "Jim Hines", "Mexico City, Mexico", "October 14, 1968"],
    [9.93, "Calvin Smith", "Colorado Springs, USA", "July 3, 1983"],
    [9.93, "Carl Lewis", "Rome, Italy", "August 30, 1987"],
    [9.92, "Carl Lewis", "Seoul, South Korea", "September 24, 1988"],
]
# intentionally overlap two of the results
wr_100m_2 = [
    [9.93, "Carl Lewis", "Rome, Italy", "August 30, 1987"],
    [9.92, "Carl Lewis", "Seoul, South Korea", "September 24, 1988"],
    [9.90, "Leroy Burrell", "New York, USA", "June 14, 1991"],
    [9.86, "Carl Lewis", "Tokyo, Japan", "August 25, 1991"],
    [9.85, "Leroy Burrell", "Lausanne, Switzerland", "July 6, 1994"],
    [9.84, "Donovan Bailey", "Atlanta, USA", "July 27, 1996"],
    [9.79, "Maurice Greene", "Athens, Greece", "June 16, 1999"],
    [9.77, "Asafa Powell", "Athens, Greece", "June 14, 2005"],
    [9.77, "Asafa Powell", "Gateshead, England", "June 11, 2006"],
    [9.77, "Asafa Powell", "Zurich, Switzerland", "August 18, 2006"],
    [9.74, "Asafa Powell", "Rieti, Italy", "September 9, 2007"],
    [9.72, "Usain Bolt", "New York, USA", "May 31, 2008"],
    [9.69, "Usain Bolt", "Beijing, China", "August 16, 2008"],
    [9.58, "Usain Bolt", "Berlin, Germany", "August 16, 2009"],
]
bins = {"scores": wr_100m_1}

try:
    client.put(key, bins, {"ttl": aerospike.TTL_NEVER_EXPIRE})
    # client.list_set_order(key, 'scores', list_policy)
    ops = [lh.list_set_order("scores", aerospike.LIST_ORDERED)]
    client.operate(key, ops)
except e.RecordError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))
    sys.exit(3)

pp = pprint.PrettyPrinter(indent=2)
try:
    list_policy = {
        "list_order": aerospike.LIST_ORDERED,
        "write_flags": (
            aerospike.LIST_WRITE_ADD_UNIQUE
            | aerospike.LIST_WRITE_PARTIAL
            | aerospike.LIST_WRITE_NO_FAIL
        ),
    }
    ops = [lh.list_append_items("scores", wr_100m_2, list_policy)]
    client.operate(key, ops)

    k, m, b = client.get(key)
    print("\nThe merged, ordered, unique scores are")
    pp.pprint(b["scores"])

    print("\nClosest two results to 10.0 seconds by relative rank")
    ops = [
        lh.list_get_by_value_rank_range_relative(
            "scores",
            [10.00, aerospike.null()],
            -1,
            aerospike.LIST_RETURN_VALUE,
            2,
            False,
        )
    ]
    k, m, b = client.operate(key, ops)
    print(b["scores"])
    print("\n")
except e.RecordError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))

client.close()
