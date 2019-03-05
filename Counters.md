# Aerospike Counter Examples

The following are examples for a tech talk on implementing counters in Aerospike.

## Consolidated Counters

In this example, a Map is used to consolidated multiple counters under one key.
This can be used to keep track of all the ads of a particular campaign that are
shown to the user in a single day.

```
python consolidated-counter.py -h "172.16.60.131" -n mem
```

See: [consolidated-counter.py](consolidated-counter.py)


## Hot Counters

In this example, a key that has become a hot counter (i.e. throwing KEY BUSY
error code 14) is split into a series of 'shards'. Those shards are then
randomly chosen and incremented separately, and read in a batch.

```
python hot-counter.py -h "172.16.60.131" -n mem
```

See: [hot-counter.py](hot-counter.py)

