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
    log.track(0, "Retrieve All Pending 'C&C' Messages.", True)

success, cnc_msgs = bc.receive(1, 5)

# For Test:
#success = [ True, None, None ]
# With msgpack()
#msg_b64 = "AAEBGGl42mteVZCfU5menxefmcK9PDUvMSknNYVxRVlqUUlmcmrx5KbFOYklpx0sD/1tW/GHd0lOfl766QMhPG6OOh9+QeWMynUXKv26DpULaDuS8LZhIVTOQT31b9etXTA5Aw7Htw+lAIMDOKYQWXjaa1uZl5+SGl9SWZC6uDgzb0VmXnFJYl5yKuPa4qLkeLjcgjUgLkySYXVeakFmfH5pSUFpCeOygsSixNziSU0rwIz4zBSWpWWJOaWpXAgRVohICgCJYC8y"

# Without msgpack()
#msg_b64 = "AAH8ARh0eJxFytEKwjAMQNF/yXMNydqksb8iIlPLGJRWdAgy+u8qyHy83LPCrZXX1OppvkJidpDreC75Gw6e+b7Ml/yAdFihjAukQTAqq31maXWCtDNC3nM06+5n2DCoSNiIKjJJ9LoRP6CnaH8RMAoJhX7sb3bvKNcQZnicTYzLCoAgFET/ZdZ3YVAbfyVCRF1cqJukBhH+e69F7oYzZ+aErD6YfMQAjcQCAkvKVtwNOkLanGkVfOh3FEFCZLOWHEt+N9FudknQ4/lFwx66J+x2Ls+rqtQ2Q9OoOtULj5kxJg=="

# Proc Node Only
#msg_b64 = "EGZ4nE2MywqAIBRE/2XWd2FQG38lQkRdXKibpAYR/nuvRe6GM2fmhKw+mHzEAI3EAgJLylbcDTpC2pxpFXzodxRBQmSzlhxLfjfRbnZJ0OP5RcMeuifsdi7Pq6rUNkPTqDrVC4+ZMSY="

# With no compression
#msg_b64 = "EJR7Im5vZGVfdHlwZSI6InNpbiIsImluc3RhbmNlIjoxLCJzcmNfbm9kZV90eXBlIjoiIiwic3JjX2luc3RhbmNlIjowLCJuZXBpX291dHB1dCI6MSwicGFyYW1zIjpbeyJwYXJhbV9pZCI6NCwidmFsdWUiOjEwfSx7InBhcmFtX2lkIjo1LCJ2YWx1ZSI6MTAwfV19"

#msg_raw = msg_b64.decode('base64')
#cnc_msgs = [ msg_raw ]
#cnc_msgs = str(msg_raw)

if not success[0]:
    if cfg.tracking:
        log.track(1, "Error(s) Receiving Cloud Messages; Close Comms.", True)
    success = bc.close(2)
    if cfg.tracking:
        log.track(0, "EXIT the Bot-Recv Subsystem.", True)
    sys.exit(1)
elif (cnc_msgs == None) or (len(cnc_msgs) == 0):
    if cfg.tracking:
        log.track(1, "NO Cloud Messages in Queue.", True)
        log.track(14, "len: " + str(len(cnc_msgs)), True)
        log.track(14, "cnc: None", True)
    success = bc.close(2)
    if cfg.tracking:
        log.track(0, "EXIT the Bot-Recv Subsystem.", True)
    sys.exit(0)
    
if cfg.tracking:
    log.track(1, "Cloud Messages Received.", True)
    log.track(14, "num: " + str(len(cnc_msgs)), True)
    log.track(0, "Take Action on ALL Downlink Messages.", True)

########################################################################
##  Parse C&C Downlink Messages and Take Appropriate Actions.
########################################################################

sdk_action = False

for msgnum in range(0, len(cnc_msgs)):
    msg = cnc_msgs[msgnum]
    msg_hex = str(msg).encode('hex')
    msg_pos = 0
    msg_len = len(msg)
    if cfg.tracking:
        log.track(1, "Evaluating DL Message: " + str(msgnum), True)
        log.track(2, "msg_hex:  [" + str(msg_hex) + "]", True)
        log.track(14, "msg_pos:  [" + str(msg_pos) + "]", True)
        log.track(14, "msg_siz:  [" + str(msg_len) + "]", True)

    while msg_pos < msg_len:
        msg_head = struct.unpack('>I', msg[msg_pos:msg_pos+4])[0]
        msg_prot = (msg_head & 0xc0000000) >> 30
        msg_type = (msg_head & 0x38000000) >> 27
        msg_size = (msg_head & 0x07ff0000) >> 16
        msg_indx = (msg_head & 0x0000fc00) >> 10
        msg_flag = (msg_head & 0x00000300) >> 8

        if cfg.tracking:
            log.track(2, "Message Header: ", True)
            log.track(15, "prot:  [" + str(int(msg_prot)) + "]", True)
            log.track(15, "type:  [" + str(int(msg_type)) + "]", True)
            log.track(15, "size:  [" + str(int(msg_size)) + "]", True)
            log.track(15, "indx:  [" + str(int(msg_indx)) + "]", True)
            log.track(15, "flag:  [" + str(int(msg_flag)) + "]", True)

        msg_pos += 3

        if cfg.tracking:
            log.track(2, "Message Data.", True)
            log.track(15, "pos:  [" + str(msg_pos) + "]", True)
        
        #-----------------------------------------------------------------------
        # Handle COMMAND Downlink Messages
        #-----------------------------------------------------------------------
        if msg_type == 0:   # COMMAND
            if cfg.tracking:
                log.track(3, "Command Message.", True)
            #msg_fmt = ">" +str(msg_size) + "B"
            msg_fmt = ">B"
            msg_cmd = struct.unpack(msg_fmt, msg[msg_pos:msg_pos+1])[0]
            if cfg.tracking:
                log.track(15, "fmt: [" + str(msg_fmt) + "]", True)
                log.track(15, "cmd: [" + str(msg_cmd) + "]", True)

            if msg_cmd == 1:
                if cfg.tracking:
                    log.track(2, "Handle 'Scuttle' Message.", True)

                try:
                    ffile = str(nepi_home)+ "/commands/scuttle"
                    success = writeFloatFile(cfg, log,3, True, ffile, str(""))
                except Exception as e:
                    if cfg.tracking:
                        log.track(3, "Error(s) Writing 'scuttlee' File.", True)
                        log.track(3, "ERROR: [" + str(e) + "]", True)

            else:
                if cfg.tracking:
                    log.track(2, "WARNING: Unknown Command Message: " + str(msg_cmd), True)
                    log.track(2, "Continue.", True)

            msg_pos += 1
            sdk_action = True
            continue

        elif msg_type == 1:   # SENSOR
            if cfg.tracking:
                log.track(2, "Handle 'Sensor' Message.", True)
                log.track(3, "NOT IMPLEMENTED YET.", True)
            msg_pos += msg_size
            continue

        elif msg_type == 2:   # NODE
            if cfg.tracking:
                log.track(2, "Handle 'Node' Message.", True)

            seg_fmt = ">" +str(msg_size) + "B"
            segment = msg[msg_pos:msg_pos+msg_size]
            seg_len = len(segment)
            seg_hex = segment.encode('hex')

            if cfg.tracking:
                log.track(15, "seg_fmt: [" + str(seg_fmt) + "]", True)
                log.track(15, "segment: [" + str(segment) + "]", True)
                log.track(15, "seg_len: [" + str(seg_len) + "]", True)
                log.track(15, "seg_hex: [" + str(seg_hex) + "]", True)

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

            # Write the proc_node file

            try:
                fname = "proc_node_cfg_" + str(msg_indx).zfill(5) + ".json"
                ffile = str(nepi_home)+ "/proc_nodes/" + str(fname)
                seg_parsed = json.loads(segment)
                seg_dumped = json.dumps(seg_parsed, indent=4, sort_keys=False)
                success = writeFloatFile(cfg, log,3, True, ffile, str(seg_dumped))
            except Exception as e:
                if cfg.tracking:
                    log.track(3, "Error(s) Writing 'proc_node'File.", True)
                    log.track(3, "ERROR: [" + str(e) + "]", True)

            msg_pos += msg_size
            sdk_action = True
            continue
            
        elif msg_type == 3:   # GEOFENCE
            if cfg.tracking:
                log.track(2, "Handle 'GeoFence' Message.", True)

            seg_fmt = ">" +str(msg_size) + "B"
            segment = msg[msg_pos:msg_pos+msg_size]
            seg_len = len(segment)
            seg_hex = segment.encode('hex')

            if cfg.tracking:
                log.track(15, "seg_fmt: [" + str(seg_fmt) + "]", True)
                log.track(15, "segment: [" + str(segment) + "]", True)
                log.track(15, "seg_len: [" + str(seg_len) + "]", True)
                log.track(15, "seg_hex: [" + str(seg_hex) + "]", True)

            #seg_dcom = zlib.decompress(segment)
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

            msg_pos += msg_size
            continue

        elif msg_type == 4:   # RULE
            if cfg.tracking:
                log.track(2, "Handle 'Rule' Message.", True)
                log.track(3, "NOT IMPLEMENTED YET.", True)
            msg_pos += msg_size
            continue

        elif msg_type == 5:   # TRIGGER
            if cfg.tracking:
                log.track(2, "Handle 'Rule' Message.", True)
                log.track(3, "NOT IMPLEMENTED YET.", True)
            msg_pos += msg_size
            continue

        else:
            #---------------------------------------------------------------
            # Handle "UNKNOWN" Message TYPE
            #---------------------------------------------------------------
            if cfg.tracking:
                log.track(2, "WARNING: Unknown Downlink Message TYPE; Continue.", True)

            msg_pos += msg_size

        break

########################################################################
# If Messages Received and on FLoat, Tell SDK.
########################################################################
  
if (cfg.state == "fl") and (sdk_action == True):
    if cfg.tracking:
        log.track(0, "Notify the SDK.", True)

    try:
        sdkproc = '/opt/numurus/ros/nepi-utilities/process-updates.sh'
        devnull = open(os.devnull, 'wb')
        Popen(['nohup', str(sdkproc)], stdout=devnull, stderr=devnull)

    except Exception as e:
        if cfg.tracking:
            log.track(1, "Error(s) Executing 'sdk' Shell.", True)
            log.track(1, "ERROR: [" + str(e) + "]", True)

########################################################################
# Application Complete: Close Comms and EXIT the Bot-Recv Subsystem.
########################################################################

if cfg.tracking:
    log.track(0, "DONE Processing Downlink Messages; Close Comms.", True)

success = bc.close(1)

if cfg.tracking:
    log.track(0, "EXIT the 'BotRecv' Subsystem.", True)

sys.exit(0)

