# Aerospike Modeling Examples

This repository contains examples of how Aerospike's complex [data types](https://www.aerospike.com/docs/guide/data-types.html),
in particular [Map](https://www.aerospike.com/docs/guide/cdt-map.html)
and [List](https://www.aerospike.com/docs/guide/cdt-list.html) can be used to
implement common patterns.

I gave a couple of tech talks at Aerospike User Group meetups which expand on these modeling approaches:
* https://www.slideshare.net/RonenBotzer/asug-tlvmeetup1talk
* https://www.slideshare.net/RonenBotzer/asug-tlvmeetup2talk

Also see [Counter Examples](counters.md).

**Note:** starting with Aerospike version 4.6 the map and list API calls can be
applied at an arbitrary depth by specifying the [context](https://www.aerospike.com/docs/guide/cdt-context.html)
for the operation. I have provided a [code sample for operating on nested CDTs](#operating-on-nested-maps-and-lists) below.

* [Operations on Nested Data Types in Aerospike](https://medium.com/@rbotzer/operations-on-nested-data-types-in-aerospike-part-i-c17400ffc15b)

## Using Maps to Capture and Query Events

In this example, a KV-ordered Map is used to collect a user's events.
The events are keyed on the millisecond timestamp of the event, with the value
being a list of the following structure:
```
timestamp: [ event-type, { map-of-all-other-event-attributes } ]
```
For example:
```python
event = {
    1523474236006: ['viewed', {'foo':'bar', 'sku':3, 'zz':'top'}] # represents a single event
}
```

This enables several types of searches using the Map API methods:
 * Get the event in a timestamp range (time slicing)
 * Get all the events of a specific type (*Note:* needs Aerospike database >= 4.3.1)
 * Get all the events matching a list of specified types (Also needs Aerospike database >= 4.3.1)

The [map return type](https://www.aerospike.com/apidocs/python/aerospike.html#map-return-types)
can be key-value pairs or the count of the elements matching the specified
'query' criteria, or something else that matters to the applicaiton developer.
```
$ python event_capture_and_query.py -h "172.16.60.131"
```

See: [event_capture_and_query.py](event_capture_and_query.py)

## Maps as Event Containers with Timestamp Values

In this example, a KV-ordered Map is used to track messages in a conversation
between two users. Each message has a UUID, which will act as the key. The
message is logged to a point in time, with a timestamp as the first index of
a list value:

```
UUID: [ timestamp, { map-of-all-other-event-attributes } ]
```
For example:
```python
message = {
    '319fa1a6-0640-4354-a426-10c4d3459f0a': [1523474316003, "Hee-hee!"]
}
```

This data modeling enables searches for messages by:
 * Get messages in a timestamp range (time slicing)
 * Get messages by their rank (most recent N messages)

Because the timestamp is not the key we can use a less fine grain resolution
(seconds rather than milliseconds) and have multiple messages with the same
timestamp.

```
$ python event_query_by_value_interval.py -h "172.16.60.131"
```

See: [event_query_by_value_interval.py](event_query_by_value_interval.py)

## Capped Collection of Events
Expanding on the previous examples of capturing and querying events in a map, we
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

The following happens in this example:
 * Initialize a collection of 6 Pac-Man high scores
 * Get the scores, sorted by date, then sorted by rank
 * Expland the top scores list to 100 with 94 new randomly generated elements
 * Show the highest and lowest of the top-100 high scores
 * Inject a new top score, keeping the map capped to 100 elements, **in a single operation** (using [operate()](https://www.aerospike.com/apidocs/python/client.html#aerospike.Client.operate))
 * Show that the number of elements remains at 100, same highest top score, and the new score at the bottom of the top-100

```
$ python capped_events.py -h "172.16.60.131"
```

See: [capped_events.py](capped_events.py)

## Ordered List Leaderboard
A collection of elements that has rank as the most important identifier, and
where that rank can repeat, may be expressed as an ordered list.

This example collects the Men's 100m world record, each element with the
following structure.

```python
[9.92, "Carl Lewis", "Seoul, South Korea", "September 24, 1988"]
```

This list structure allows for elements to be found by rank, even if some
elements share the exact same rank.

The example shows how duplicates can be avoided with write modify flags, while
at the same time allowing for equal ranked elements.

### Querying by Relative Rank
One of the most interesting features of the List API is getting values by
relative rank.

In this example there is no element whose rank is exactly 10.0. Using the
relative rank method, the adjacent values are returned with a single call.

```python
Closest two results to 10.0 seconds
[[9.95, 'Jim Hines', 'Mexico City, Mexico', 'October 14, 1968'], [10.02, 'Charles Greene', 'Mexico City, Mexico', 'October 13, 1968']]
```

```
$ python ordered_list_leaderboard.py -h "172.16.60.131"
```

See: [ordered_list_leaderboard.py](ordered_list_leaderboard.py)

## Operating on Nested Maps and Lists
In the previous examples, operations were applied to the top level elements of
a list or map. As of Aerospike version 4.6 this is no longer a limit. A map and
list API operation can be applied at an arbitrary depth described by a
[context](https://www.aerospike.com/docs/guide/cdt-context.html) object.

See the Python client documentation for [defining](https://aerospike-python-client.readthedocs.io/en/latest/aerospike_helpers.html#module-aerospike_helpers.cdt_ctx) and [using](https://aerospike-python-client.readthedocs.io/en/latest/aerospike_helpers.operations.html#module-aerospike_helpers.operations.map_operations) a context.

In this example, a leaderboard is constructed as a map whose keys are player IDs,
and whose value contains a tuple (list). The first element of this tuple
is the player's score, the second being a map of attributes. We will be applying
operations on this embedded attributes map.

```python
{ "CPU": [9800, {"dt": "2017-12-05 01:01:11", "ts": 1512435671573}] }
```

```
$ python nested_cdts.py -h "172.16.60.131"
```

See: [nested_cdts.py](nested_cdts.py)
