# -*- coding: utf-8 -*-
from __future__ import print_function
import aerospike
from aerospike import exception as e
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
    "-s", "--set", dest="set", type="string", default="scores_demo", metavar="<SET>",
    help="Port of the Aerospike server.")
(options, args) = optparser.parse_args()
if options.help:
    optparser.print_help()
    print()
    sys.exit(1)

def random_score():
    year = random.randrange(1981, 2017)
    month = random.randrange(1, 12)
    if month == 2:
        if not calendar.isleap(year):
            day = random.randrange(1, 28, 1)
        else:
            day = random.randrange(1, 29, 1)
    elif month in [4, 6, 9, 11]:
        day = random.randrange(1, 30, 1)
    else:
        day = random.randrange(1, 31, 1)
    hour = random.randrange(0, 23, 1)
    minute = random.randrange(0, 59, 1)
    second = random.randrange(0, 59, 1)
    dt = datetime.datetime(year, month, day, hour, minute, second)
    ms = random.randrange(100, 999, 1)
    ts = int(dt.strftime('%s')) *1000 + ms
    name = chr(random.randrange(65, 90, 1)) + chr(random.randrange(65, 90, 1)) + chr(random.randrange(65, 90, 1))
    score = random.randrange(2000, 18390, 10)
    return (ts, score, name, dt.strftime('%Y-%m-%d %H:%M:%S'))

config = {'hosts': [(options.host, options.port)]}
try:
    client = aerospike.client(config).connect(
                options.username, options.password)
except ex.ClientError as e:
  print("Error: {0} [{1}]".format(e.msg, e.code))
  sys.exit(2)

namespace = options.namespace if options.namespace and options.namespace != 'None' else None
set = options.set if options.set and options.set != 'None' else None
key = (namespace, set, 'pacman')
scores = {
    1512435671573: [9800,  {'name': 'CPU', 'dt': '2017-12-05 01:01:11'}],
    1525182446891: [9200,  {'name': 'ETC', 'dt': '2018-05-01 13:47:26'}],
    1515114071923: [24700, {'name': 'SOS', 'dt': '2018-01-05 01:01:11'}],
    291807988156:  [34500, {'name': 'ACE', 'dt': '1980-10-26 09:46:28'}],
    1521398652483: [18400, {'name': 'EIR', 'dt': '2018-03-18 18:44:12'}],
    1511104958197: [17400, {'name': 'CFO', 'dt': '2017-11-19 15:22:38'}],
}
bins = {'scores': scores}

try:
    client.put(key, bins, {'ttl': aerospike.TTL_NEVER_EXPIRE})
    map_policy = {
        'map_write_mode': aerospike.MAP_UPDATE,
        'map_order': aerospike.MAP_KEY_VALUE_ORDERED
    }
    client.map_set_policy(key, 'scores', map_policy)
except e.RecordError as e:
  print("Error: {0} [{1}]".format(e.msg, e.code))
  sys.exit(3)

try:
    pp = pprint.PrettyPrinter(indent=2)
    meta = {'ttl': aerospike.TTL_NEVER_EXPIRE}
    print("\nGet the Pac-Man top scores, from an initial list of 6, sorted by date")
    by_date = client.map_get_by_index_range(key, 'scores', 0, -1, aerospike.MAP_RETURN_KEY_VALUE)
    pp.pprint(by_date)
    print("\nGet all Pac-Man top scores, sorted by score (rank)")
    by_rank = client.map_get_by_rank_range(key, 'scores', 0, -1, aerospike.MAP_RETURN_KEY_VALUE)
    pp.pprint(by_rank)
    print("\nExpanding top scores for Pac-Man with 94 randomly generated elements")
    for i in xrange(6, 100, 1):
        ts, score, name, dt = random_score()
        scores[ts] = [score, {'name': name, 'dt': dt}]
    client.map_put_items(key, 'scores', scores, map_policy, meta)
    print("Confirming the size of Pac-Man top scores:", client.map_size(key, 'scores'))
    print("\nGet the top-10 Pac-Man scores, sorted by score (rank)")
    by_rank = client.map_get_by_rank_range(key, 'scores', -10, -1, aerospike.MAP_RETURN_KEY_VALUE)
    pp.pprint(by_rank)
    print("\nLowest score is:")
    pp.pprint(client.map_get_by_rank(key, 'scores', 0, aerospike.MAP_RETURN_KEY_VALUE))
    print("Highest score is:")
    pp.pprint(client.map_get_by_rank(key, 'scores', -1, aerospike.MAP_RETURN_KEY_VALUE))
    print("\nInject a new top score, keep map capped to 100 elements, in a single operation")
    ts, score, name, dt = random_score()
    new_score = [score, {'name': name, 'dt': dt}]
    ops = [
        {
            "op" : aerospike.OP_MAP_PUT,
            "bin": "scores",
            "key": ts,
            "val": new_score
        },
        {
            "op" : aerospike.OP_MAP_REMOVE_BY_RANK,
            "bin": "scores",
            "index": 0
        }
    ]
    client.operate(key, ops, meta)
    print("\nSize of the top scores map is", client.map_size(key, 'scores'))
    print("New lowest score is:")
    pp.pprint(client.map_get_by_rank(key, 'scores', 0, aerospike.MAP_RETURN_KEY_VALUE))
    print("Highest score remains:")
    pp.pprint(client.map_get_by_rank(key, 'scores', -1, aerospike.MAP_RETURN_KEY_VALUE))
    client.truncate(namespace, set, 0)
except e.RecordError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))

client.close()
