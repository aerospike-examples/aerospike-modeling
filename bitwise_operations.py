# -*- coding: utf-8 -*-
from args import options
import aerospike
from aerospike import exception as ex
from aerospike_helpers.operations import bitwise_operations
from aerospike_helpers.operations import operations
import datetime
import sys
import time

spacer = "=" * 30

def version_tuple(version):
    return tuple(int(i) for i in version.split("."))

if options.set == "None":
    options.set = None
config = {"hosts": [(options.host, options.port)]}
try:
    client = aerospike.client(config).connect(options.username, options.password)
except ex.ClientError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))
    sys.exit(2)

version = client.info_all("version")
release = list(version.values())[0][1].split(" ")[-1]
if version_tuple(aerospike.__version__) < version_tuple("3.10.0") or version_tuple(
    release
) < version_tuple("4.7.0.2"):
    print(
        "\nThe background scan and bitwise operations require",
        "Aerospike database 4.7 / Python client 3.10.0.",
        "Please update to run this example.",
    )
    sys.exit(3)

key1 = (options.namespace, options.set, "bit-example1")
key2 = (options.namespace, options.set, "bit-example2")
try:
    client.remove(key1)
except ex.RecordError as e:
    pass
try:
    client.remove(key2)
except ex.RecordError as e:
    pass

try:
    # data model:
    # ntotal | nmonth | nweek | ntoday | dminutes (day-minutes-bitfield)

    # you can initialize the record with a 1440-bit bitfield of zeros
    day_bitfield = bytearray(180)
    client.put(
        key1,
        {"ntotal": 0, "nmonth": 0, "nweek": 0, "ntoday": 0, "dminutes": day_bitfield},
    )

    # then set one specific bit, minute 1421
    buff = (255).to_bytes(1, sys.byteorder)  # a byte buffer, all bits raised
    client.operate(
        key1,
        [
            operations.increment("ntotal", 1),
            operations.increment("nmonth", 1),
            operations.increment("nweek", 1),
            operations.increment("ntoday", 1),
            bitwise_operations.bit_set("dminutes", 1421, 1, 1, buff),
        ],
    )

    # otherwise, conditionally create the bitfield ahead of the operation
    bit_policy = {
        "bit_write_flags": aerospike.BIT_WRITE_CREATE_ONLY | aerospike.BIT_WRITE_NO_FAIL
    }
    client.operate(
        key2,
        [
            operations.increment("ntotal", 1),
            operations.increment("nmonth", 1),
            operations.increment("nweek", 1),
            operations.increment("ntoday", 1),
            bitwise_operations.bit_insert("dminutes", 0, 180, day_bitfield, bit_policy),
            bitwise_operations.bit_set("dminutes", 1421, 1, 1, buff),
        ],
    )

    # two different ways to check if bit 1421
    # first, ask for a 1 bit length bitfield at bit 1421
    k, m, b = client.operate(key2, [bitwise_operations.bit_get("dminutes", 1421, 1)])
    byte = b["dminutes"][0]
    print("get_bit(1421) returned {}".format(bin(byte)))
    # got one byte padded with zeros after the returned bitfield
    x = 0b10000000
    print("is the bit raised? {}".format(byte == x))
    print(spacer)

    # next, use a bitwise count
    k, m, b = client.operate(key1, [bitwise_operations.bit_count("dminutes", 1421, 1)])
    count = b["dminutes"]
    print("bit count at bit 1421 is {}".format(count))
    print(spacer)

    # find the most recent event of the day
    k, m, b = client.operate(
        key1, [bitwise_operations.bit_rscan("dminutes", 0, 1440, True)]
    )
    print("most recent event was at minute {} (using rscan)".format(b["dminutes"]))
    print(spacer)

    # let's add a bin for tracking days
    # data model:
    # ntotal | nmonth | nweek | ntoday | dminutes (bitfield) | recent368 (bitfield)
    recent368_bitfield = bytearray(46)
    client.put(key1, {"recent368": recent368_bitfield})

    # any time we have an event we raise the last bit
    bit_policy = {"bit_write_flags": aerospike.BIT_WRITE_NO_FAIL}
    k, m, b = client.operate(
        key1, [bitwise_operations.bit_set("recent368", 367, 1, 1, buff)]
    )

    # find the first event of the past 90 days. there are 368 bits, so scan
    # between 278-367 for the leftmost bit
    k, m, b = client.operate(
        key1, [bitwise_operations.bit_lscan("recent368", 278, 90, True)]
    )
    first = 89 - b["recent368"]
    print("most recent event was {} days ago (using lscan)".format(first))
    k, m, b = client.get(key1)
    print(b["recent368"])
    print(spacer)

    # once a day we will left-shift the bits of the recent368 bitfield
    k, m, b = client.operate(
        key1,
        [
            bitwise_operations.bit_lshift("recent368", 0, 368, 1),
            bitwise_operations.bit_lscan("recent368", 278, 90, True),
        ],
    )
    first = 89 - b["recent368"]
    print("after the lshift, the most recent event was {} days ago".format(first))
    k, m, b = client.get(key1)
    print(b["recent368"])
    print(spacer)

    # the daily left-shift can be launched via background scan
    scan = client.scan(options.namespace, options.set)
    scan.add_ops([bitwise_operations.bit_lshift("recent368", 0, 368, 1)])
    job_id = scan.execute_background()
    # wait for job to finish
    while True:
        response = client.job_info(job_id, aerospike.JOB_SCAN)
        if response["status"] != aerospike.JOB_STATUS_INPROGRESS:
            break
        time.sleep(0.1)
    print("background scan execute finished")
    k, m, b = client.operate(
        key1, [bitwise_operations.bit_lscan("recent368", 278, 90, True)]
    )
    first = 89 - b["recent368"]
    print(
        "after the background scan lshift, the most recent event was {} days ago".format(
            first
        )
    )
    k, m, b = client.get(key1)
    print(b["recent368"])
except ex.ClientError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))

client.close()
