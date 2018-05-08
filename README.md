# Aerospike CDT Examples

This repository contains examples of how Aerospike's complex [data types](https://www.aerospike.com/docs/guide/data-types.html),
in particular [Map](https://www.aerospike.com/docs/guide/cdt-map.html)
and [List](https://www.aerospike.com/docs/guide/cdt-list.html) can be used to
implement common patterns.

## Using Maps to Capture and Query Events

A KV-ordered Map is used to collect a user's events. The events are keyed on
the millisecond timestamp of the event, with the value being a list of the
following structure:
```
[ event-type, { map-of-all-other-event-attributes } ]
```
For example:
```python
event = {
    1523474236006: ['viewed', {'foo':'bar', 'sku':3, 'zz':'top'}] # represents a single event
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
$ python event_capture_and_query.py --help
Usage: event_capture_and_query.py [options]

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

$ python event_capture_and_query.py -h "172.16.60.131"
```

## Capped Collection of Events
Expanding on the previous example of capturing and querying events in a map, we
will see how a collection (map or list) can be capped to a specified size.

In this example we're tracking high scores for video games over time. The
KV-sorted map has a milisecond timestamp for a key, and list of the following
structure for a value:
```
[score,  {'name': name, 'dt': YYYY-MM-DD HH:mm:ss}]
```
For example:
```python
score = {
    1512435671573: [9800,  {'name': 'CPU', 'dt': '2017-12-05 01:01:11'}]
}
```

In the example the following happens:
 * Initialize a collection of 6 Pac-Man high scores
 * Get the scores, sorted by date, then sorted by rank
 * Expland the top scores list to 100 with 94 new randomly generated elements
 * Show the highest and lowest of the top-100 high scores
 * Inject a new top score, keeping the map capped to 100 elements, **in a single operation** (using [operate()](https://www.aerospike.com/apidocs/python/client.html#aerospike.Client.operate))
 * Show that the number of elements remains at 100, same highest top score, and the new score at the bottom of the top-100

```
$ python capped_events.py --help
Usage: capped_events.py [options]

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

$ python capped_events.py -h "172.16.60.131"
```

