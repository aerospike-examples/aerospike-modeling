# -*- coding: utf-8 -*-
from __future__ import print_function
import aerospike
from aerospike import exception as e
from optparse import OptionParser
import pprint
import sys

# Options Parsing
usage = "usage: %prog [options]"
optparser = OptionParser(usage=usage, add_help_option=False)
optparser.add_option(
    "--help", dest="help", action="store_true",
    help="Displays this message.")
optparser.add_option(
    "-U", "--username", dest="username", type="string", metavar="<USERNAME>",
    help="Username to connect to database.")
optparser.add_option(
    "-P", "--password", dest="password", type="string", metavar="<PASSWORD>",
    help="Password to connect to database.")
optparser.add_option(
    "-h", "--host", dest="host", type="string", default="127.0.0.1", metavar="<ADDRESS>",
    help="Address of Aerospike server.")
optparser.add_option(
    "-p", "--port", dest="port", type="int", default=3000, metavar="<PORT>",
    help="Port of the Aerospike server.")
optparser.add_option(
    "-n", "--namespace", dest="namespace", type="string", default="test", metavar="<NS>",
    help="Port of the Aerospike server.")
optparser.add_option(
    "-s", "--set", dest="set", type="string", default="cdt_demo", metavar="<SET>",
    help="Port of the Aerospike server.")
(options, args) = optparser.parse_args()
if options.help:
    optparser.print_help()
    print()
    sys.exit(1)

config = {'hosts': [(options.host, options.port)]}
try:
    client = aerospike.client(config).connect(
                options.username, options.password)
except e.ClientError as e:
  print("Error: {0} [{1}]".format(e.msg, e.code))
  sys.exit(2)

namespace = options.namespace if options.namespace and options.namespace != 'None' else None
set = options.set if options.set and options.set != 'None' else None
key = (namespace, set, '5bc47d70-76fa-b531-2e7d9013b831')
messages = {
    '0edf5b73-535c-4be7-b653-c0513dc79fb4': [1523474230000, "Billie Jean is not my lover",
                                             "Michael Jackson", ""],
    '29342a0b-e20f-4676-9ecf-dfdf02ef6683': [1523474241001, "She's just a girl who claims that I am the one",
                                             "Michael Jackson", "0edf5b73-535c-4be7-b653-c0513dc79fb4"],
    '9f54b4f8-992e-427f-9fb3-e63348cd6ac9': [1523474249006, "...",
                                             "Tito Jackson", "29342a0b-e20f-4676-9ecf-dfdf02ef6683"],
    '1ae56b18-7a3c-4f64-adb7-2e845eb5094e': [1523474257005, "But the kid is not my son",
                                             "Michael Jackson", "9f54b4f8-992e-427f-9fb3-e63348cd6ac9"],
    '08785e96-eb1b-4a74-a767-7b56e8f13ea9': [1523474306005, "ok...",
                                             "Tito Jackson", "1ae56b18-7a3c-4f64-adb7-2e845eb5094e"],
    '319fa1a6-0640-4354-a426-10c4d3459f0a': [1523474316003, "Hee-hee!",
                                             "Michael Jackson", "08785e96-eb1b-4a74-a767-7b56e8f13ea9"],
}

bins = {'messages': messages, 'users': ['Michael Jackson', 'Tito Jackson'],
        'subject': 'Billie Jean'
}

try:
    client.put(key, bins, {'ttl': aerospike.TTL_NEVER_EXPIRE})
    map_policy = {
        'map_write_mode': aerospike.MAP_UPDATE,
        'map_order': aerospike.MAP_KEY_VALUE_ORDERED
    }
    client.map_set_policy(key, 'messages', map_policy)
except e.RecordError as e:
  print("Error: {0} [{1}]".format(e.msg, e.code))
  sys.exit(3)

try:
    pp = pprint.PrettyPrinter(indent=2)
    print("\nGet the messages between two timestamps using 'get_by_value_interval'")
    k = client.map_get_by_value_range(key, 'messages',
            [1523474230000, aerospike.null()], [1523474250000, aerospike.null()],
            aerospike.MAP_RETURN_VALUE)
    pp.pprint(k)
    print("\nGet the four most recent messages using 'get_by_rank_range'")
    v = client.map_get_by_rank_range(key, 'messages', -4, 4, aerospike.MAP_RETURN_VALUE)
    pp.pprint(v)
except e.RecordError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))

client.close()
