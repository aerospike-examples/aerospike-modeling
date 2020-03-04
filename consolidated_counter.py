# -*- coding: utf-8 -*-
from __future__ import print_function
from args import options
import aerospike
from aerospike import exception as ex
from aerospike_helpers.operations import map_operations
import pprint
import random
import sys

config = {"hosts": [(options.host, options.port)]}
try:
    client = aerospike.client(config).connect(options.username, options.password)
except ex.ClientError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))
    sys.exit(2)

if options.set == "None":
    options.set = None
key = (
    options.namespace,
    options.set,
    "cf296d9a-0b77-4dd0-8d2b-91e59a6f02d2|campaign123|2019-03-05",
)
pp = pprint.PrettyPrinter(indent=2)
AD_LIMIT = 4
ads = ["adx123", "adz345", "ady987", "ado717"]

try:
    client.remove(key)
except:
    pass

for i in range(0, 20, 1):
    try:
        ad = ads[random.randrange(0, 4, 1)]
        val = client.map_get_by_key(key, "ads", ad, aerospike.MAP_RETURN_VALUE)
        print("Ad", ad, "has value", val)
        if not val:
            val = 0
        if val < AD_LIMIT:
            # continue and attempt to serve the ad
            print("Ad", ad, "is under the cap, incrementing its counter")
            client.map_increment(
                key, "ads", ad, 1, {}, {"ttl": aerospike.TTL_DONT_UPDATE}
            )  # aka ttl -2
    except ex.RecordNotFound as e:
        ttl = aerospike.TTL_NAMESPACE_DEFAULT  # AKA ttl 0, inherit the default-ttl
        try:
            ops = [
                map_operations.map_put("ads", ad, 1, {}),
                map_operations.map_get_by_key("ads", ad, aerospike.MAP_RETURN_VALUE),
            ]
            print("Ad", ad, "does not exist yet, initializing it to 1")
            (key, meta, bins) = client.operate(key, ops, {"ttl": ttl})
        except ex.AerospikeError as e:
            print("Error: {0} [{1}]".format(e.msg, e.code))

print("\nState of the consolidated counters at this key")
k, m, b = client.get(key)
pp.pprint(b)
client.close()
