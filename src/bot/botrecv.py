########################################################################
##
##  Module: botrecv.py
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
##  Revision:   1.2 2019/03/22  15:50:00
##  Comment:    Integration with latest botcomm.py.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
##  Revision:   1.1 2019/03/22  13:00:00
##  Comment:    Module Instantiation; Complete Revision.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
########################################################################

import os
import sys
import time
import uuid
import json
import math
import shutil
import socket
import sqlite3
from botdefs import nepi_home
from array import array
from botcfg import BotCfg
from botlog import BotLog
from botdb import BotDB
from botmsg import BotMsg
from botcomm import BotComm
from bothelp import writeFloatFile

########################################################################
##  Instantiate a BotCfg Configuration Class Object (from 'botcfg.py')
########################################################################

cfg = BotCfg()
cfg.initcfg()

########################################################################
##  Instantiate a BotLog Debug/Log Class Object (from 'botlog.py')
########################################################################

log = BotLog(cfg, "BOT-RECV")
log.initlog(0)

########################################################################
##  Instantiate a BotDB Database Class (from 'botdb.py').
########################################################################

db = BotDB(cfg, log, 0)
success, dbconn = db.getconn(0)

if not success[0]:
    sys.exit(1)

########################################################################
# Instantiate a BotComm Communication Class Object (from 'botcomm.py').
########################################################################

bc = BotComm(cfg, log, "Iridium", 0)
conn_success = bc.getconn(0)

########################################################################
##  Retrieve any Pending Downlink Messages.
########################################################################

if conn_success[0]:
    if cfg.tracking:
        log.track(0, "Retrieve All Pending 'C&C' Messages.", True)

    success, cnc_msgs = bc.receive(1)

if not conn_success[0]:
    pass
elif not success[0]:
    if cfg.tracking:
        log.track(1, "Error(s) Receiving Cloud Messages", True)
elif len(cnc_msgs) == 0:
    if cfg.tracking:
        log.track(1, "No Cloud Messages in Queue.", True)
        log.track(2, "len: " + str(len(cnc_msgs)), True)
        log.track(2, "cnc: " + str(cnc_msgs), True)
else:
    if cfg.tracking:
        log.track(1, "Cloud Messages Received.", True)
        log.track(2, "len: " + str(len(cnc_msgs)), True)
        log.track(2, "cnc: " + str(cnc_msgs), True)
        log.track(1, "Construct 'Action' File for SDK.", True)

    #-------------------------------------------------------------------
    # Construct 'Action' File
    #-------------------------------------------------------------------

    action_data = {"action_seq_id": "31", "actions": [{"action_id": "5", "max_duration": "1000"}]}

    action_json = json.dumps(action_data, indent=4, sort_keys=True, separators=(",", ": "), ensure_ascii=False)

    action_num  = 1
    action_nstr = '{:05d}'.format(action_num)
    action_file = nepi_home + "/cfg/action/action_seq_" + action_nstr + ".json"
    if cfg.tracking:
        log.track(2, "action_num:  " + str(action_num), True)
        log.track(2, "action_nstr: " + str(action_nstr), True)
        log.track(2, "action_file: " + str(action_file), True)
        log.track(8, "action_data: " + str(action_data), True)
        log.track(8, "action_json: " + str(action_json), True)
        log.track(1, "Write the 'Action' file for the SDK.", True)

    #---------------------------------------------------------------
    # Write 'Action' File

    if cfg.tracking:
        log.track(1, "Write the 'Action' file for the SDK.", True)

    success = writeFloatFile(action_file, action_json)
    if success[0]:
        if cfg.tracking:
            log.track(2, "Successful.", True)
    else:
        if cfg.tracking:
            log.track(2, "ERROR " + str(success[1] + ": " + success[2]), True)

    #---------------------------------------------------------------
    # Construct 'Task' File

    if cfg.tracking:
        log.track(1, "Construct 'Task' File for SDK.", True)

    task_data = {"line": "55", "action_seq_id": "31", "start_time": "0.0", "period": "900", "max_repetition": "-1"}

    task_json = json.dumps(task_data, indent=4, sort_keys=False, separators=(",", ": "), ensure_ascii=False)

    task_num  = 1
    task_nstr = '{:05d}'.format(task_num)
    task_file = nepi_home + "/cfg/sched/task_" + action_nstr + ".json"
    if cfg.tracking:
        log.track(2, "task_num:  " + str(task_num), True)
        log.track(2, "task_nstr: " + str(task_nstr), True)
        log.track(2, "task_file: " + str(task_file), True)
        log.track(8, "task_data: " + str(task_data), True)
        log.track(8, "task_json: " + str(task_json), True)
        log.track(1, "Write the 'Task' file for the SDK.", True)

    #---------------------------------------------------------------
    # Write 'Task' File

    success = writeFloatFile(task_file, task_json)
    if success[0]:
        if cfg.tracking:
            log.track(2, "Successful.", True)
    else:
        if cfg.tracking:
            log.track(2, "ERROR " + str(success[1] + ": " + success[2]), True)


########################################################################
# Close the Bot-Recv Subsystem.
########################################################################

success = bc.close(0)

########################################################################
# Close the Bot-Recv Subsystem.
########################################################################
if cfg.tracking:
    log.track(0, "", True)
    log.track(0, "EXIT the Bot-Recv Subsystem.", True)
    log.track(0, "", True)

sys.exit(0)

