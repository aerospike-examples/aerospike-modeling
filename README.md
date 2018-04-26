# Aerospike CDT Examples

This repository contains examples of how Aerospike's complex [data types](https://www.aerospike.com/docs/guide/data-types.html),
in particular [Map](https://www.aerospike.com/docs/guide/cdt-map.html)
and [List](https://www.aerospike.com/docs/guide/cdt-list.html) can be used to
implement common patterns.

## Map Examples

A KV-ordered Map is used to collect a user's events. The events are keyed on
the millisecond timestamp of the event, with the value being a list of the
following structure:
```
[ event-type, { map-of-all-other-attributes } ]
```
For example:
```python
event = {
    1523474236006: ['viewed', {'foo':'bar', 'sku':3, 'zz':'top'}]
}
```

This enables several types of searches using the Map API methods:
 * Get the event in a timestamp range (time slicing).
 * Get all the events of a specific type
 * Get all the events matching a list of specified types

The [map return type](https://www.aerospike.com/apidocs/python/aerospike.html#map-return-types)
can be key-value pairs or the count of the elements matching the specified
'query' criteria, or something else that matters to the applicaiton developer.
```
$ python map_example.py --help
Usage: map_example.py [options]

Options:
  --help                Displays this message.
  -U <USERNAME>, --username=<USERNAME>
                        Username to connect to database.
  -P <PASSWORD>, --password=<PASSWORD>
                        Password to connect to database.
  -h <ADDRESS>, --host=<ADDRESS>
                        Address of Aerospike server.
  -p <PORT>, --port=<PORT>
                        Port of the Aerospike server.
  -n <NS>, --namespace=<NS>
                        Port of the Aerospike server.
  -s <SET>, --set=<SET>
                        Port of the Aerospike server.

$ python map_example.py -h "172.16.60.131"
```
