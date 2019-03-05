########################################################################
##
##  Module: bot-defs.py
##  --------------------
##
##  (c) Copyright 2019 by Numurus LLC
##
##  This document, and all information therein, is the property of
##  Numurus LLC.  It is confidential and must not be made public or
##  copied in any form.  It is loaned subject to return upon demand
##  and is not to be used directly or indirectly in any way detrimental
##  to our interests.
##
##  Revision History
##  ----------------
##  
##  Revision:   1.2 2019/02/22  11:30:00
##  Comment:    Revised primary file lcoations and logging flag.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
##  Revision:   1.1 2019/02/04  09:15:00
##  Comment:    Module Instantiation.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
########################################################################

#import os
#import sys
#import time
import uuid
#import json
#from pylocker import Locker

########################################################################
##  Useful Global Variable Definitions.
########################################################################

# Identify running platform using a Python2 kludge for mocking Enums.
# ALPHA represents local (developer) testing on private platforms; TEST
# represent Numurus testing; FLOAT represents active, Float production.
# The range of 1-2-3 is arbitrary.  Set 'Machine' to desired bed.
class Machines:
    ALPHA, TEST, FLOAT = range(1, 4)

Machine = Machines.ALPHA    # Set default to ALPHA local.
Machine = Machines.TEST     # Set default to TEST remote.
#Machine = Machines.FLOAT    # Set default to FLOAT live.

# Directory and File Locations.  NEPI Home should be /usr/nepi-usr on
# the Float itself (per the "NumSDK - NEPI-Bot ICD"). For a variety of
# reasons, the Bot Configuration and Log Files should be static to
# insure acccessibility.
nepi_home = "../.."
bot_cfg_file = nepi_home + "/" + "cfg/bot/config.json"  # Bot Cfg File
bot_db_file  = nepi_home + "/" + "db/float.db"          # Bot DB File

# This is another Python2 way of doing Enums.  We don't seem to have the
# retro implementation from Python3, so use either a forced "class" or
# the method presented here.
def enum(**values):
    return type('Enum', (), values)

# Create a unique lock pass for subsequent file locking. This can be any
# string but, for convenience, we can grab the uuid from the system in a
# uniform manner.
try:
    lockpass = str(uuid.uuid1())
except:
    lockpass = "NumurusLockPass2019"

# Define Global Convenience Booleans for debugging, logging, timing,
# tracking, and locking.
debugging = False
logging = False
timing = False
tracking = False
locking = False
