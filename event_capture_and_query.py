# -*- coding: utf-8 -*-
from __future__ import print_function
from args import options
import aerospike
from aerospike import exception as ex
import pprint
import sys


def version_tuple(version):
    return tuple(int(i) for i in version.split("."))


config = {"hosts": [(options.host, options.port)]}
try:
    client = aerospike.client(config).connect(options.username, options.password)
except ex.ClientError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))
    sys.exit(2)

if options.set == "None":
    options.set = None
key = (options.namespace, options.set, 1)
events = {
    1523474230000: ["fav", {"sku": 1, "b": 2}],
    1523474231001: ["comment", {"sku": 2, "b": 22}],
    1523474236006: ["viewed", {"foo": "bar", "sku": 3, "zz": "top"}],
    1523474235005: ["comment", {"sku": 1, "c": 1234}],
    1523474233003: ["viewed", {"sku": 3, "z": 26}],
    1523474234004: ["viewed", {"sku": 1, "ff": "hhhl"}],
}

bins = {"events": events, "yes": "way"}

try:
    client.put(key, bins, {"ttl": aerospike.TTL_NEVER_EXPIRE})
    map_policy = {
        "map_write_mode": aerospike.MAP_UPDATE,
        "map_order": aerospike.MAP_KEY_VALUE_ORDERED,
    }
    client.map_set_policy(key, "events", map_policy)
except ex.RecordError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))
    sys.exit(3)

pp = pprint.PrettyPrinter(indent=2)
try:
    version = client.info_all("version")
    release = list(version.values())[0][1].split(" ")[-1]
    # The following works starting Aerospike database 4.3.1 and client 3.5.0
    if version_tuple(aerospike.__version__) >= version_tuple("3.5.0") and version_tuple(
        release
    ) >= version_tuple("4.3.1"):
        print("\nGet all the 'comment' type events")
        v = client.map_get_by_value(
            key,
            "events",
            ["comment", aerospike.CDTWildcard()],
            aerospike.MAP_RETURN_KEY_VALUE,
        )
        pp.pprint(v)
        print("\nGet the count of all the 'viewed' type events")
        c = client.map_get_by_value(
            key,
            "events",
            ["viewed", aerospike.CDTWildcard()],
            aerospike.MAP_RETURN_COUNT,
        )
        print(c)
        print("\nGet the count of all the 'viewed' and 'comment' type events")
        c = client.map_get_by_value_list(
            key,
            "events",
            [["viewed", aerospike.CDTWildcard()], ["comment", aerospike.CDTWildcard()]],
            aerospike.MAP_RETURN_COUNT,
        )
        print(c)
    print("\nGet all the events in the range between two millisecond timestamps")
    k = client.map_get_by_key_range(
        key, "events", 1523474230000, 1523474235999, aerospike.MAP_RETURN_KEY_VALUE
    )
    pp.pprint(k)
    o = client.info_all("sets/{0}/{1}".format(options.namespace, options.set))
    object_size = list(o.values())[0][1].split(":")[2]
    print("\nThe size of the object with 6 events:")
    print(object_size)
except ex.RecordError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))
    sys.exit(4)

print("\nAdding cruft")
events = {}
t = 1523473230000
for i in range(1, 401, 1):
    events[t] = ["zzz", {"sku": i, "bbb": i}]
    t = t + 7337
t = 1523474237007
for i in range(401, 801, 1):
    events[t] = ["xyz", {"sku": i, "bbb": i}]
    t = t + 7139

try:
    client.map_put_items(key, "events", events, map_policy)
    c = client.map_size(key, "events")
    print("\nChecking for the number of events")
    print(c)
    print("400 elements before the inital 6 events, 400 after")
    if version_tuple(aerospike.__version__) >= version_tuple("3.5.0") and version_tuple(
        release
    ) >= version_tuple("4.3.1"):
        print("\nGet the count of all the 'viewed' and 'comment' type events")
        c = client.map_get_by_value_list(
            key,
            "events",
            [["viewed", aerospike.CDTWildcard()], ["comment", aerospike.CDTWildcard()]],
            aerospike.MAP_RETURN_COUNT,
        )
        print(c)
        print("\nGet all the 'comment' type events")
        v = client.map_get_by_value(
            key,
            "events",
            ["comment", aerospike.CDTWildcard()],
            aerospike.MAP_RETURN_KEY_VALUE,
        )
        pp.pprint(v)
        print("\nGet the count of all the 'viewed' type events")
        c = client.map_get_by_value(
            key,
            "events",
            ["viewed", aerospike.CDTWildcard()],
            aerospike.MAP_RETURN_COUNT,
        )
        print(c)
    print("\nGet all the events in the range between two millisecond timestamps")
    k = client.map_get_by_key_range(
        key, "events", 1523474230000, 1523474235999, aerospike.MAP_RETURN_KEY_VALUE
    )
    pp.pprint(k)
    o = client.info_all("sets/{0}/{1}".format(options.namespace, options.set))
    object_size = list(o.values())[0][1].split(":")[2]
    print("\nThe size of the object with 806 events:")
    print(object_size)
    client.truncate(options.namespace, options.set, 0)
except ex.RecordError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))

client.close()
