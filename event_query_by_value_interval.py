# -*- coding: utf-8 -*-
from __future__ import print_function
from args import options
import aerospike
from aerospike import exception as ex
import pprint
import sys


config = {"hosts": [(options.host, options.port)]}
try:
    client = aerospike.client(config).connect(options.username, options.password)
except ex.ClientError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))
    sys.exit(2)

if options.set == "None":
    options.set = None
key = (options.namespace, options.set, "5bc47d70-76fa-b531-2e7d9013b831")
messages = {
    "0edf5b73-535c-4be7-b653-c0513dc79fb4": [
        1523474230000,
        "Billie Jean is not my lover",
        "Michael Jackson",
        "",
    ],
    "29342a0b-e20f-4676-9ecf-dfdf02ef6683": [
        1523474241001,
        "She's just a girl who claims that I am the one",
        "Michael Jackson",
        "0edf5b73-535c-4be7-b653-c0513dc79fb4",
    ],
    "9f54b4f8-992e-427f-9fb3-e63348cd6ac9": [
        1523474249006,
        "...",
        "Tito Jackson",
        "29342a0b-e20f-4676-9ecf-dfdf02ef6683",
    ],
    "1ae56b18-7a3c-4f64-adb7-2e845eb5094e": [
        1523474257005,
        "But the kid is not my son",
        "Michael Jackson",
        "9f54b4f8-992e-427f-9fb3-e63348cd6ac9",
    ],
    "08785e96-eb1b-4a74-a767-7b56e8f13ea9": [
        1523474306005,
        "ok...",
        "Tito Jackson",
        "1ae56b18-7a3c-4f64-adb7-2e845eb5094e",
    ],
    "319fa1a6-0640-4354-a426-10c4d3459f0a": [
        1523474316003,
        "Hee-hee!",
        "Michael Jackson",
        "08785e96-eb1b-4a74-a767-7b56e8f13ea9",
    ],
}

bins = {
    "messages": messages,
    "users": ["Michael Jackson", "Tito Jackson"],
    "subject": "Billie Jean",
}

try:
    client.put(key, bins, {"ttl": aerospike.TTL_NEVER_EXPIRE})
    map_policy = {
        "map_write_mode": aerospike.MAP_UPDATE,
        "map_order": aerospike.MAP_KEY_VALUE_ORDERED,
    }
    client.map_set_policy(key, "messages", map_policy)
except ex.RecordError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))
    sys.exit(3)

pp = pprint.PrettyPrinter(indent=2)
try:
    print("\nGet the messages between two timestamps using", "'get_by_value_interval'")
    k = client.map_get_by_value_range(
        key,
        "messages",
        [1523474230000, aerospike.null()],
        [1523474250000, aerospike.null()],
        aerospike.MAP_RETURN_VALUE,
    )
    pp.pprint(k)
    print("\nGet the four most recent messages using 'get_by_rank_range'")
    v = client.map_get_by_rank_range(key, "messages", -4, 4, aerospike.MAP_RETURN_VALUE)
    pp.pprint(v)
except ex.RecordError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))

client.close()
