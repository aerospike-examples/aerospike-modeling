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
    "-i", "--iterations", dest="num", type="int", default=15,
    metavar="<ITERATIONS>", help="Number of interations.")
optparser.add_option(
    "-c", "--cap", dest="cap", type="int", default=4,
    metavar="<CAP>", help="Max ads shown to a users.")
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
key = (namespace, set, 'cf296d9a-0b77-4dd0-8d2b-91e59a6f02d2|adx123|2019-03-05')
pp = pprint.PrettyPrinter(indent=2)
AD_LIMIT = 4

# setup - overload the key's counter with a string and shard the original
# record
shards = []
try:
    for i in range(1, 6, 1):
        k = (namespace, set, 'cf296d9a-0b77-4dd0-8d2b-91e59a6f02d2|adx123' +
                             '|2019-03-05||' + str(i))
        shards.append(k)
        try:
          client.remove(k)
        except:
          pass
    client.put(key, {'c': 0})
    client.remove(key)
except Exception as err:
    pp.pprint(err)
    pass
client.put(key, {'c': '🚀'})

try:
  print("One way to identify this key is now an indicator for a shareded",
        "counter is to try and increment it, then catch the exception")
  client.increment(key, 'c', 1, {}, {'ttl': aerospike.TTL_DONT_UPDATE})
except e.BinIncompatibleType as err:
    print("Caught the error code 12 BinIncompatibleType exception")
    k, m, b = client.get(key)
    print("The original key contains ", list(b.values())[0], "instead of an integer")

print("\nThe non-EAFP approach is to check for a shard key's existence")
k, meta = client.exists(shards[0])
pp.pprint(meta)


for i in range(0, options.num, 1):
  try:
    shard_key = shards[random.randrange(0, 5, 1)]
    k, m, b = client.get(shard_key)
    val = list(b.values())[0]
    print("Shard", shard_key[2], "has value", val)
    if not val:
        val = 0
    if val < options.cap:
      # continue and attempt to serve the ad
      print("The shard is under the cap, incrementing its counter")
      client.increment(shard_key, 'c', 1, {}, {'ttl': aerospike.TTL_DONT_UPDATE}) # aka ttl -2
  except e.RecordNotFound as err:
    ttl = aerospike.TTL_NAMESPACE_DEFAULT # AKA ttl 0, inherit the default-ttl
    try:
      ops = [
        oh.increment('c', 1),
        oh.read('c')
      ]
      print("The shard", shard_key[2], "does not exist yet, initializing it to 1")
      (shard_key, meta, bins) = client.operate(shard_key, ops, {'ttl': ttl})
    except e.AerospikeError as err:
      print("Error: {0} [{1}]".format(err.msg, err.code))

print("\nState of the sharded counters")
for i in range(0, 5, 1):
  shard_key = shards[i]
  try:
    k, m, b = client.get(shard_key)
    print("shard", shard_key[2], "=", list(b.values())[0])
  except e.RecordNotFound as err:
    print("shard", shard_key[2], "was never used")

# batch read the shards and combine the value
records = client.get_many(shards)
total = 0
for k, m, b in records:
    if m:
      total = total + list(b.values())[0]
print("The combined count is ", total)
k, m, b = client.get(key)
print("State of the original key is still", list(b.values())[0])
client.close()
