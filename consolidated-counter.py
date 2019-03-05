# -*- coding: utf-8 -*-
from __future__ import print_function
import aerospike
from aerospike import exception as e
from aerospike_helpers.operations import list_operations as lh
from aerospike_helpers.operations import map_operations as mh
from aerospike_helpers.operations import operations as oh
from optparse import OptionParser
import pprint
import random
import sys

# Options Parsing
usage = "usage: %prog [options]"
optparser = OptionParser(usage=usage, add_help_option=False)
optparser.add_option(
    "--help", dest="help", action="store_true", help="Displays this message.")
optparser.add_option(
    "-U", "--username", dest="username", type="string", metavar="<USERNAME>",
    help="Username to connect to database.")
optparser.add_option(
    "-P", "--password", dest="password", type="string", metavar="<PASSWORD>",
    help="Password to connect to database.")
optparser.add_option(
    "-h", "--host", dest="host", type="string", default="127.0.0.1",
    metavar="<ADDRESS>", help="Address of Aerospike server.")
optparser.add_option(
    "-p", "--port", dest="port", type="int", default=3000, metavar="<PORT>",
    help="Port of the Aerospike server.")
optparser.add_option(
    "-n", "--namespace", dest="namespace", type="string", default="test",
    metavar="<NS>", help="Port of the Aerospike server.")
optparser.add_option(
    "-s", "--set", dest="set", type="string", default="adcap",
    metavar="<SET>", help="Port of the Aerospike server.")
optparser.add_option(
    "-i", "--iterations", dest="num", type="int", default=20,
    metavar="<ITERATIONS>", help="Number of interations.")
options, args = optparser.parse_args()
if options.help:
    optparser.print_help()
    print()
    sys.exit(1)

config = {'hosts': [(options.host, options.port)]}
try:
    client = aerospike.client(config).connect(
        options.username, options.password)
except e.ClientError as err:
  print("Error: {0} [{1}]".format(err.msg, err.code))
  sys.exit(2)

if options.namespace and options.namespace != 'None':
    namespace = options.namespace
else:
    namespace = None
set = options.set if options.set and options.set != 'None' else None
key = (namespace, set, 'cf296d9a-0b77-4dd0-8d2b-91e59a6f02d2|campaign123|2019-03-05')
pp = pprint.PrettyPrinter(indent=2)
AD_LIMIT = 4
ads = ['adx123', 'adz345', 'ady987', 'ado717']

try:
    client.remove(key)
except:
    pass

for i in range (0, options.num, 1):
  try:
    ad = ads[random.randrange(0, 4, 1)]
    val = client.map_get_by_key(key, 'ads', ad, aerospike.MAP_RETURN_VALUE)
    print("Ad", ad, "has value", val)
    if not val:
        val = 0
    if val < AD_LIMIT:
      # continue and attempt to serve the ad
      print("Ad", ad, "is under the cap, incrementing its counter")
      client.map_increment(key, 'ads', ad, 1, {}, {'ttl': aerospike.TTL_DONT_UPDATE}) # aka ttl -2
  except e.RecordNotFound as err:
    ttl = aerospike.TTL_NAMESPACE_DEFAULT # AKA ttl 0, inherit the default-ttl
    try:
      ops = [
        mh.map_put('ads', ad, 1, {}),
        mh.map_get_by_key('ads', ad, aerospike.MAP_RETURN_VALUE)
      ]
      print("Ad", ad, "does not exist yet, initializing it to 1")
      (key, meta, bins) = client.operate(key, ops, {'ttl': ttl})
    except e.AerospikeError as err:
      print("Error: {0} [{1}]".format(err.msg, err.code))

print("\nState of the consolidated counters at this key")
k, m, b = client.get(key)
pp.pprint(b)
client.close()
