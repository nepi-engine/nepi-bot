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
##  Revision:   1.4 2019/05/01  15:50:00
##  Comment:    New bot61; rewrite; added all C&C management.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
##  Revision:   1.3 2019/04/29  15:50:00
##  Comment:    Major revsion 5/1 to handle downlink messsaging.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
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
import ast
from subprocess import Popen
#import msgpack
import struct
#import zlib
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


#def inflate(data):
    #decompress = zlib.decompressobj(
            #-zlib.MAX_WBITS  # see above
    #)
    #inflated = decompress.decompress(data)
    #inflated += decompress.flush()
    #return inflated

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

if success[0]:
    dbActiveFlag = True         # Flag reserved to future use.
else:
    dbActiveFlag = False

if cfg.tracking:
    log.track(1, "DB Active Status: " + str(dbActiveFlag), True)

########################################################################
# Instantiate a BotComm Communication Class Object (from 'botcomm.py').
########################################################################

bc = BotComm(cfg, log, cfg.type, 0)
success = bc.getconn(0)

if not success[0]:
    if cfg.tracking:
        log.track(0, "EXIT the 'BotRecv' Subsystem.", True)
    sys.exit(1)

########################################################################
##  Retrieve any Pending C&C Downlink Messages.
########################################################################

if cfg.tracking:
    log.track(0, "Retrieve All Pending 'C&C' Downlink Messages.", True)

success, cnc_msgs = bc.receive(1, 5)

# For Test
if not cfg.comms:
    success = [ True, None, None ]
    # With msgpack()
    #msg_b64 = "AAEBGGl42mteVZCfU5menxefmcK9PDUvMSknNYVxRVlqUUlmcmrx5KbFOYklpx0sD/1tW/GHd0lOfl766QMhPG6OOh9+QeWMynUXKv26DpULaDuS8LZhIVTOQT31b9etXTA5Aw7Htw+lAIMDOKYQWXjaa1uZl5+SGl9SWZC6uDgzb0VmXnFJYl5yKuPa4qLkeLjcgjUgLkySYXVeakFmfH5pSUFpCeOygsSixNziSU0rwIz4zBSWpWWJOaWpXAgRVohICgCJYC8y"

    # Without msgpack()
    #msg_b64 = "AAH8ARh0eJxFytEKwjAMQNF/yXMNydqksb8iIlPLGJRWdAgy+u8qyHy83LPCrZXX1OppvkJidpDreC75Gw6e+b7Ml/yAdFihjAukQTAqq31maXWCtDNC3nM06+5n2DCoSNiIKjJJ9LoRP6CnaH8RMAoJhX7sb3bvKNcQZnicTYzLCoAgFET/ZdZ3YVAbfyVCRF1cqJukBhH+e69F7oYzZ+aErD6YfMQAjcQCAkvKVtwNOkLanGkVfOh3FEFCZLOWHEt+N9FudknQ4/lFwx66J+x2Ls+rqtQ2Q9OoOtULj5kxJg=="

    # Proc Node Only
    #msg_b64 = "EGZ4nE2MywqAIBRE/2XWd2FQG38lQkRdXKibpAYR/nuvRe6GM2fmhKw+mHzEAI3EAgJLylbcDTpC2pxpFXzodxRBQmSzlhxLfjfRbnZJ0OP5RcMeuifsdi7Pq6rUNkPTqDrVC4+ZMSY="

    # Latest for bto61
    msg_b64 = "AAH8ARiTAHsicG9seWdvbl9pZCI6MTEsImVuYWJsZWQiOjEsInZlcnRpY2VzIjpbeyJsYXQiOjI1Ljc2MTY4MSwibG9uZyI6LTgwLjE5MTc4OH0seyJsYXQiOjE4LjQ2NTU0LCJsb25nIjotNjYuMTA1NzM2fSx7ImxhdCI6MzIuMzA3OCwibG9uZyI6LTY0Ljc1MDUwNH1dfRCUAHsibm9kZV90eXBlIjoic2luIiwiaW5zdGFuY2UiOjEsInNyY19ub2RlX3R5cGUiOiIiLCJzcmNfaW5zdGFuY2UiOjAsIm5lcGlfb3V0cHV0IjoxLCJwYXJhbXMiOlt7InBhcmFtX2lkIjo0LCJ2YWx1ZSI6MTB9LHsicGFyYW1faWQiOjUsInZhbHVlIjoxMDB9XX0YkAB7InBvbHlnb25faWQiOjcsImVuYWJsZWQiOjAsInZlcnRpY2VzIjpbeyJsYXQiOjIyLjU2ODM1LCJsb25nIjotODIuMTkxNzh9LHsibGF0IjoxOS40NjUzMiwibG9uZyI6LTc2LjEwNTczNn0seyJsYXQiOjIyLjU2ODM1LCJsb25nIjotODIuMTkxNzh9XX0="

    msg_raw = msg_b64.decode('base64')
    cnc_msgs = [ msg_raw ]

    if cfg.tracking:
        log.track(1, "NO Comms Mode.", True)
        log.track(2, "msg_b64: [" + str(msg_b64) + "]", True)
        log.track(2, "len_b64: [" + str(len(msg_b64)) + "]", True)
        log.track(2, "msg_raw: [" + str(msg_raw) + "]", True)
        log.track(2, "len_raw: [" + str(len(msg_raw)) + "]", True)
        log.track(2, "msg_hex: [" + str(msg_raw).encode("hex") + "]", True)
        log.track(2, "len_hex: [" + str(len(str(msg_raw).encode("hex"))) + "]", True)


if not success[0]:
    if cfg.tracking:
        log.track(1, "Error(s) Receiving C&C Downlink Messages.", True)
        log.track(0, "DONE Processing Downlink Messages; Close Comms.", True)
    success = bc.close(1)
    if cfg.tracking:
        log.track(0, "EXIT the Bot-Recv Subsystem.", True)
    sys.exit(1)
elif (cnc_msgs == None) or (len(cnc_msgs) == 0):
    if cfg.tracking:
        log.track(1, "NO C&C Downlink Messages in Queue.", True)
        log.track(0, "DONE Processing Downlink Messages; Close Comms.", True)
    success = bc.close(1)
    if cfg.tracking:
        log.track(0, "EXIT the Bot-Recv Subsystem.", True)
    sys.exit(0)
    
if cfg.tracking:
    log.track(1, "C&C Downlink Messages Received.", True)
    log.track(14, "Total: [" + str(len(cnc_msgs)) + "]", True)
    log.track(0, "Take Action on ALL C&C Downlink Messages.", True)

########################################################################
##  Parse C&C Downlink Messages and Take Appropriate Actions.
########################################################################

sdk_action = False  # Used later to indicate we have to notify the SDK.

for msgnum in range(0, len(cnc_msgs)):
    #msg_b64 = cnc_msgs[msgnum]
    #msg = msg_b64.decode('base64')
    msg = cnc_msgs[msgnum]
    msg_pos = 0
    msg_len = len(msg)
    msg_hex = str(msg).encode('hex')
    if cfg.tracking:
        log.track(1, "Evaluating C&C Message #" + str(msgnum), True)
        log.track(2, "msg_pos: [" + str(msg_pos) + "]", True)
        log.track(2, "msg_len: [" + str(msg_len) + "]", True)
        log.track(14, "msg_hex: [" + str(msg_hex) + "]", True)

    #-------------------------------------------------------------------
    # Loop Through the C&C Segments Until Message is Exhausted.
    #-------------------------------------------------------------------
    while msg_pos < msg_len:
        # Even though msg 'header' is exactly 3 bytes, peel off 4 so
        # 'struct' can deal with it as a 32-bit integer. Ignore the
        # far-right eight bits (that's atually the 1st byte of data).
        if cfg.tracking:
            log.track(2, "Evaluating Segment Header.", True)

        try:    
            seg_head = struct.unpack('>I', msg[msg_pos:msg_pos+4])[0]
            seg_prot = (seg_head & 0xc0000000) >> 30    # 'protocol'    (bits 0-1)
            seg_type = (seg_head & 0x38000000) >> 27    # 'config type' (bits 2-4)
            seg_size = (seg_head & 0x07ff0000) >> 16    # 'msg length'  (bits 5-15)
            seg_indx = (seg_head & 0x0000fc00) >> 10    # 'cfg index'   (bits 16-21)
            seg_flag = (seg_head & 0x00000300) >> 8     # 'msg flags'   (bits 22-23)
        except Exception as e:
            if cfg.tracking:
                log.track(3, "Lost Control Evaluating This Message.", True)
                log.track(3, "ERROR: [" + str(e) + "]", True)
                log.track(3, "Continue w/next MESSAGE.", True)
            break

        if cfg.tracking:
            log.track(15, "seg_prot:  [" + str(int(seg_prot)) + "]", True)
            log.track(15, "seg_type:  [" + str(int(seg_type)) + "]", True)
            log.track(15, "seg_size:  [" + str(int(seg_size)) + "]", True)
            log.track(15, "seg_indx:  [" + str(int(seg_indx)) + "]", True)
            log.track(15, "seg_flag:  [" + str(int(seg_flag)) + "]", True)

        #---------------------------------------------------------------
        # Loop Through C&C Message Segments Until Message is Exhausted.
        #---------------------------------------------------------------
        msg_pos += 3    # Skip forward over the 3-byte segment 'header.'

        if cfg.tracking:
            log.track(2, "Bump Forward to Process Message Data.", True)
            log.track(15, "msg_pos:  [" + str(msg_pos) + "]", True)

        try:
            data_fmt = ">" +str(seg_size) + "B"
            if cfg.tracking:
                log.track(15, "data_fmt: [" + str(data_fmt) + "]", True)
            data_raw = msg[msg_pos:msg_pos+seg_size]
            if cfg.tracking:
                log.track(15, "data_raw: [" + str(data_raw) + "]", True)
            data_len = len(data_raw)
            if cfg.tracking:
                log.track(15, "data_len: [" + str(data_len) + "]", True)
            data_hex = data_raw.encode('hex')
            if cfg.tracking:
                log.track(15, "data_hex: [" + str(data_hex) + "]", True)
            data_unp = struct.unpack(data_fmt, data_raw)[0]
        except Exception as e:
            if cfg.tracking:
                log.track(3, "Problem(s) Positioning for Segment Data.", True)
                log.track(3, "ERROR: [" + str(e) + "]", True)
                log.track(3, "Continue w/next MESSAGE.", True)
            break

        # TO-DO: Work with Jacob

        #seg_dcom = zlib.decompress(str(segment))
        #seg_dcom = inflate(segment)
        #seg_dsiz = len(seg_dcom)

        #if cfg.tracking:
            #log.track(15, "dcom: [" + str(seg_dcom) + "]", True)
            #log.track(15, "dsiz: [" + str(seg_dsiz) + "]", True)

        #b = bytearray()
        #b.extend(seg_dcom)

        #seg_unpk = msgpack.load(seg_dcom, use_list=True)
        #seg_usiz = len(seg_unpk)

        #if cfg.tracking:
            #log.track(15, "unpk: [" + str(seg_unpk) + "]", True)
            #log.track(15, "usiz: [" + str(seg_usiz) + "]", True)

        #---------------------------------------------------------------
        # Construct File Path, Name, and File.
        #---------------------------------------------------------------
        if cfg.tracking:
            log.track(2, "Identify Directory and Base Name.", True)

        if seg_type == 0:                               # COMMAND
            fpath = "/commands/"
            cmd = int(str(data_unp))
            if int(cmd) == 1:                      
                fname = "scuttle"                       # Scuttle
            else:
                if cfg.tracking:
                    log.track(2, "WARNING: Got 'Unknown' Command: [" + str(cmd) + "]", True)
                    log.track(2, "No Implementation; Continue w/next SEGMENT.", True)
                msg_pos += seg_size
                continue

        elif seg_type == 1:                             # SENSOR
            fpath = "/sensors/"
            fname = "sensor_cfg"

        elif seg_type == 2:                             # NODE
            fpath = "/proc_nodes/"
            fname = "proc_node_cfg"
            
        elif seg_type == 3:                             # GEOFENCE
            fpath = "/geofence/"
            fname = "geofence_cfg"

        elif seg_type == 4:                             # RULE
            fpath = "/rules/"
            fname = "smarttrig_rule"

        elif seg_type == 5:                             # TRIGGER
            fpath = "/trig/"
            fname = "smarttrig_cfg"

        else:
            if cfg.tracking:
                log.track(2, "WARNING: Got 'Unknown' C&C Segment TYPE.", True)
                log.track(2, "No Implementation; Continue w/next SEGMENT.", True)
            msg_pos += seg_size
            continue

        if cfg.tracking:
            log.track(3, "bpath: [" + str(fpath) + "]", True)
            log.track(3, "bname: [" + str(fname) + "]", True)
            log.track(2, "Construct File Path, Name, and File.", True)

        try:
            fpath = str(nepi_home) + str(fpath)
            if cfg.tracking:
                log.track(3, "fpath: [" + str(fpath) + "]", True)
            if seg_type > 0:
                fname = str(fname) + "_" + str(seg_indx).zfill(5) + ".json"
            if cfg.tracking:
                log.track(3, "fname: [" + str(fname) + "]", True)
            ffile = str(fpath) + str(fname)
            if cfg.tracking:
                log.track(3, "ffile: [" + str(ffile) + "]", True)
            if seg_type > 0:
                fpars = json.loads(data_raw)    # Use raw data for now (is a string)
                if cfg.tracking:
                    log.track(15, "fpars: [" + str(fpars) + "]", True)
                fdump = json.dumps(fpars, indent=4, sort_keys=False)
                if cfg.tracking:
                    log.track(15, "fdump: [" + str(fdump) + "]", True)
            else:
                fpars = ""
                fdump = ""
        except Exception as e:
            if cfg.tracking:
                log.track(3, "Problem(s) Constructing C&C File.", True)
                log.track(3, "ERROR: [" + str(e) + "]", True)
                log.track(3, "Continue w/next MESSAGE.", True)
            break

        if cfg.tracking:
            log.track(2, "Create the File.", True)

        success = writeFloatFile(cfg, log, 3, True, ffile, str(fdump))
        if success[0]:
            sdk_action = True   # Got at least 1 C&C message for the SDK.
        else:
            if cfg.tracking:
                log.track(2, "ERROR: Unable to Create C&C File.", True)
                log.track(2, "No Implementation; Continue w/next SEGMENT.", True)

        msg_pos += seg_size
        continue

########################################################################
# Application Complete: Close Comms.
########################################################################

if cfg.tracking:
    log.track(0, "DONE Processing All C&C Downlink Messages; Close Comms.", True)

success = bc.close(1)

########################################################################
# SDK C&C Downlink Message Notification.
########################################################################

if cfg.tracking:
    log.track(0, "SDK C&C Downlink Message Notification.", True)

if sdk_action:
    if cfg.tracking:
        log.track(1, "Messages Processed; Proceed.", True)

    if (cfg.state == "fl"):
        sdkproc = "/opt/numurus/ros/nepi-utilities/process-updates.sh"
        sdkwhat = "Live Float Mode"
    else:
        #sdkproc = "/mnt/hgfs/Numurus/Float61/src/tst/sdktest.sh"
        #sdkproc = "../../src/tst/sdktest.sh"
        sdkproc = str(nepi_home) + "/src/tst/sdktest.sh"
        sdkwhat = "Local Non-Float Mode."

    try:
        if cfg.tracking:
            log.track(1, str(sdkwhat), True)
            log.track(1, "File: [" + str(sdkproc) + "]", True)
            log.track(1, "Setting 'devnull' device", True)
        devnull = open(os.devnull, 'wb')
        if cfg.tracking:
            log.track(1, "Execute Popen w/nohup.", True)
        if os.path.isfile(str(sdkproc)):
            #Popen([str(sdkproc)])
            #Popen(['nohup', str(sdkproc)])
            Popen(['nohup', "/bin/sh " + str(sdkproc)], stdout=devnull, stderr=devnull)
        else:
            raise("Can't find SDK Notification Process.")

        if cfg.tracking:
            log.track(1, "DONE with SDK Notification.", True)

    except Exception as e:
        if cfg.tracking:
            log.track(2, "Errors(s) Executing Local Test App.", True)
            log.track(2, "ERROR: [" + str(e) + "]", True)
            log.track(2, "Continue.", True)
else:
    if cfg.tracking:
        log.track(1, "NO Messages Processed: Ignore SDK Notification.", True)

########################################################################
# Application Complete: EXIT the Bot-Recv Subsystem.
########################################################################

if cfg.tracking:
    log.track(0, "EXIT the 'BotRecv' Subsystem.", True)

sys.exit(0)

