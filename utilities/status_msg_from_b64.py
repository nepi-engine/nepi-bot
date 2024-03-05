#
# Copyright (c) 2024 Numurus, LLC <https://www.numurus.com>.
#
# This file is part of nepi-engine
# (see https://github.com/nepi-engine).
#
# License: 3-clause BSD, see https://opensource.org/licenses/BSD-3-Clause
#
import base64 # Debugging

from google.protobuf import json_format
from google.protobuf import message

import nepi_messaging_all_pb2

b64_msg = input("Enter B64 string:\n")
binary_msg = base64.b64decode(b64_msg)

protobuf_msg = nepi_messaging_all_pb2.NEPIMsg()
protobuf_msg.ParseFromString(binary_msg)
json_msg = json_format.MessageToJson(
    protobuf_msg,
    including_default_value_fields=False,
    preserving_proto_field_name=False,
    indent=2,
)

print(json_msg)
