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
    "-s", "--set", dest="set", type="string", default="events_demo", metavar="<SET>",
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
key = (namespace, set, 1)
events = {
    1523474230000: ['fav', {'sku':1, 'b':2}],
    1523474231001: ['comment', {'sku':2, 'b':22}],
    1523474236006: ['viewed', {'foo':'bar', 'sku':3, 'zz':'top'}],
    1523474235005: ['comment', {'sku':1, 'c':1234}],
    1523474233003: ['viewed', {'sku':3, 'z':26}],
    1523474234004: ['viewed', {'sku':1, 'ff':'hhhl'}],
}

bins = {'events': events, 'yes': 'way'}

try:
    client.put(key, bins, {'ttl': aerospike.TTL_NEVER_EXPIRE})
    map_policy = {
        'map_write_mode': aerospike.MAP_UPDATE,
        'map_order': aerospike.MAP_KEY_VALUE_ORDERED
    }
    client.map_set_policy(key, 'events', map_policy)
except e.RecordError as e:
  print("Error: {0} [{1}]".format(e.msg, e.code))
  sys.exit(3)

# return types
# sneaky by value comparisons
# check value range if 1st is int requires second to be null
try:
    pp = pprint.PrettyPrinter(indent=2)
    print("\nGet all the 'comment' type events")
    v = client.map_get_by_value(key, 'events', ['comment'], aerospike.MAP_RETURN_KEY_VALUE)
    pp.pprint(v)
    print("\nGet the count of all the 'viewed' type events")
    c = client.map_get_by_value(key, 'events', ['viewed'], aerospike.MAP_RETURN_COUNT)
    print(c)
    version = client.info_all('version')
    release = version.values()[0][1].split(' ')[-1]
    if aerospike.__version__ >= '3.2.0' and release >= '3.16.0.1':
        print("\nGet the count of all the 'viewed' and 'comment' type events")
        c = client.map_get_by_value_list(key, 'events',[['viewed'], ['comment']], aerospike.MAP_RETURN_COUNT)
        print(c)
    print("\nGet all the events in the range between two millisecond timestamps")
    k = client.map_get_by_key_range(key, 'events', 1523474230000, 1523474235999, aerospike.MAP_RETURN_KEY_VALUE)
    pp.pprint(k)
    o = client.info_all("sets/{0}/{1}".format(namespace, set))
    object_size = o.values()[0][1].split(':')[2]
    print("\nThe size of the object with 6 events:")
    print(object_size)
except e.RecordError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))
    sys.exit(4)

print("\nAdding cruft")
events = {}
t = 1523473230000
for i in xrange(1, 401, 1):
    events[t] = ['zzz', {'sku':i,'bbb':i}]
    t = t + 7337
t = 1523474237007
for i in xrange(401, 801, 1):
    events[t] = ['xyz', {'sku':i,'bbb':i}]
    t = t + 7139

try:
    client.map_put_items(key, 'events', events, map_policy)
    c = client.map_size(key, 'events')
    print("\nChecking for the number of events")
    print(c)
    print("400 elements before the inital 6 events, 400 after")
    print("\nGet all the 'comment' type events")
    v = client.map_get_by_value(key, 'events', ['comment'], aerospike.MAP_RETURN_KEY_VALUE)
    pp.pprint(v)
    print("\nGet the count of all the 'viewed' type events")
    c = client.map_get_by_value(key, 'events', ['viewed'], aerospike.MAP_RETURN_COUNT)
    print(c)
    if aerospike.__version__ >= '3.2.0' and release >= '3.16.0.1':
        print("\nGet the count of all the 'viewed' and 'comment' type events")
        c = client.map_get_by_value_list(key, 'events',[['viewed'], ['comment']], aerospike.MAP_RETURN_COUNT)
        print(c)
    print("\nGet all the events in the range between two millisecond timestamps")
    k = client.map_get_by_key_range(key, 'events', 1523474230000, 1523474235999, aerospike.MAP_RETURN_KEY_VALUE)
    pp.pprint(k)
    o = client.info_all("sets/{0}/{1}".format(namespace, set))
    object_size = o.values()[0][1].split(':')[2]
    print("\nThe size of the object with 806 events:")
    print(object_size)
    client.truncate(namespace, set, 0)
except e.RecordError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))

client.close()
