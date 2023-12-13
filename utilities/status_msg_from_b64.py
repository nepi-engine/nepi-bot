#
# NEPI Dual-Use License
# Project: nepi-bot
#
# This license applies to any user of NEPI Engine software
#
# Copyright (C) 2023 Numurus, LLC <https://www.numurus.com>
# see https://github.com/numurus-nepi/nepi-bot
#
# This software is dual-licensed under the terms of either a NEPI software developer license
# or a NEPI software commercial license.
#
# The terms of both the NEPI software developer and commercial licenses
# can be found at: www.numurus.com/licensing-nepi-engine
#
# Redistributions in source code must retain this top-level comment block.
# Plagiarizing this software to sidestep the license obligations is illegal.
#
# Contact Information:
# ====================
# - https://www.numurus.com/licensing-nepi-engine
# - mailto:nepi@numurus.com
#
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
