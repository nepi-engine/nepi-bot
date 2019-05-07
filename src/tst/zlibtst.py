import base64
import zlib
import msgpack
rawMsgPack = "a1uZl5+SGl9SWZC6uDgzb0VmXnFJYl5yKuPa4qLkeLjcgjUgLkySYXVeakFmfH5pSUFpCeOygsSixNziSU0rwIz4zBSWpWWJOaWpXAgRVohICgA="
msgMsgPack = base64.b64decode(rawMsgPack)
def inflate(data):
    decompress = zlib.decompressobj(
        -zlib.MAX_WBITS  # see above
    )
    inflated = decompress.decompress(data)
    inflated += decompress.flush()
    unpacked = msgpack.unpackb(inflated)
    return unpacked
print(inflate(msgMsgPack))


