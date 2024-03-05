#
# Copyright (c) 2024 Numurus, LLC <https://www.numurus.com>.
#
# This file is part of nepi-engine
# (see https://github.com/nepi-engine).
#
# License: 3-clause BSD, see https://opensource.org/licenses/BSD-3-Clause
#
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


