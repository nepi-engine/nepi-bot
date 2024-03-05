#
# Copyright (c) 2024 Numurus, LLC <https://www.numurus.com>.
#
# This file is part of nepi-engine
# (see https://github.com/nepi-engine).
#
# License: 3-clause BSD, see https://opensource.org/licenses/BSD-3-Clause
#

# import os
# import sys
# import time
import uuid

# import json
# from pylocker import Locker

########################################################################
# Useful Global Variable Definitions.
########################################################################

v_botdefs = "bot71-20200601"


# Identify running platform using a Python2 kludge for mocking Enums.
# ALPHA represents local (developer) testing on private platforms; TEST
# represent Numurus testing; FLOAT represents active, Float production.
# The range of 1-2-3 is arbitrary.  Set 'Machine' to desired bed.


class Machines:
    ALPHA, TEST, FLOAT = range(1, 4)


# Machine = Machines.ALPHA    # Set default to ALPHA local.
# Machine = Machines.TEST     # Set default to TEST remote.
Machine = Machines.FLOAT  # Set default to FLOAT live.


class Messages:
    STATUS, META, STANDARD, CHANGE = range(1, 5)


# Directory and File Locations.  NEPI Home should be /usr/nepi-usr on
# the Float itself (per the "NumSDK - NEPI-Bot ICD"). For a variety of
# reasons, the Bot Configuration, Log and Device-specific Files must be
# static to insure insure accessibility.
nepi_home = "../.."
bot_cfg_file = nepi_home + "/cfg/bot/config.json"  # Bot Cfg File
bot_db_file = nepi_home + "/db/float.db"  # Bot DB File
bot_devnuid_file = nepi_home + "/devinfo/devnuid.txt"
bot_devsshkeys_file = nepi_home + "/devinfo/devsshkeys.txt"
bot_hb_dir = nepi_home + "/hb"


# This is another Python2 way of doing Enums.  We don't seem to have the
# retro implementation from Python3, so use either a forced "class" or
# the method presented here.


def enum(**values):
    return type("Enum", (), values)


# Create a unique lock pass for subsequent file locking. This can be any
# string but, for convenience, we can grab the uuid from the system in a
# uniform manner.
try:
    lockpass = str(uuid.uuid1())
except Exception as e:
    lockpass = "NumurusLockPass2021"

# Define Global Convenience Booleans for debugging, logging, timing,
# tracking, and locking.
debugging = False
logging = False
timing = False
tracking = False
locking = False

# Define global variables that are used by different modules

msgs_incoming = list()
msgs_outgoing = list()

udp_packet_trace = 0  # trace the packetizing and sending of the messages
msg_gen_trace = 0  # trace the original message generation and packing
