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
    log.track(0, "Retrieve All Pending 'C&C' Downlink Messages.", True)

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
        log.track(1, "Error(s) Receiving C&C Downlink Messages.", True)
        log.track(1, "Close Comms.", True)
    success = bc.close(2)
    if cfg.tracking:
        log.track(0, "EXIT the Bot-Recv Subsystem.", True)
    sys.exit(1)
elif (cnc_msgs == None) or (len(cnc_msgs) == 0):
    if cfg.tracking:
        log.track(1, "NO C&C Downlink Messages in Queue.", True)
        log.track(1, "Close Comms.", True)
    success = bc.close(2)
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
        log.track(1, "Evaluating C&C Downlink Message #" + str(msgnum), True)
        log.track(2, "msg_pos: [" + str(msg_pos) + "]", True)
        log.track(2, "msg_len: [" + str(msg_len) + "]", True)
        log.track(14, "msg_hex: [" + str(msg_hex) + "]", True)

    #-------------------------------------------------------------------
    # Loop Through C&C Message Segments Until this Message is Exhausted.
    #-------------------------------------------------------------------
    while msg_pos < msg_len:
        # Even though msg 'header' is exactly 3 bytes, peel off 4 so
        # 'struct' can deal with it as a 32-bit integer. Ignore the
        # far-right eight bits (that's atually the 1st byte of data).
        if cfg.tracking:
            log.track(2, "Evaluating C&C Segment Header.", True)

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
                log.track(3, "Continue w/NEXT MESSAGE.", True)
            break

        msg_pos += 3    # Skip forward over the 3-byte segment 'header.'

        if cfg.tracking:
            log.track(2, "Evaluating C&C Segment Header.", True)
            log.track(15, "seg_prot:  [" + str(int(seg_prot)) + "]", True)
            log.track(15, "seg_type:  [" + str(int(seg_type)) + "]", True)
            log.track(15, "seg_size:  [" + str(int(seg_size)) + "]", True)
            log.track(15, "seg_indx:  [" + str(int(seg_indx)) + "]", True)
            log.track(15, "seg_flag:  [" + str(int(seg_flag)) + "]", True)
            log.track(2, "Bump Forward to Process Message Data.", True)
            log.track(15, "msg_pos:  [" + str(msg_pos) + "]", True)
        
        #---------------------------------------------------------------
        # Handle 'Command' C&C Segment TYPE
        #---------------------------------------------------------------
        if seg_type == 0:   # COMMAND
            if cfg.tracking:
                log.track(2, "Handle 'Command' C&C Segment TYPE.", True)
            # We only need to peel off a single., unsigned byte for any
            # 'command.'  The 'struct' method returns a list - in this
            # case, a single list element (0).
            seg_fmt = ">B"
            seg_cmd = struct.unpack(seg_fmt, msg[msg_pos:msg_pos+1])[0]
            if cfg.tracking:
                log.track(15, "seg_fmt: [" + str(seg_fmt) + "]", True)
                log.track(15, "seg_cmd: [" + str(seg_cmd) + "]", True)

            if seg_cmd == 1:
                # The 'scuttle' command require only that we create an
                # empty file, called 'scuttle,' in the "commands/" dir.
                if cfg.tracking:
                    log.track(2, "Handle C&C 'Scuttle' Command.", True)

                try:
                    ffile = str(nepi_home) + "/commands/scuttle"
                    success = writeFloatFile(cfg, log,3, True, ffile, str(""))
                    sdk_action = True   # Got at least 1 C&C message for the SDK.
                except Exception as e:
                    if cfg.tracking:
                        log.track(3, "Problem(s) Creating 'scuttle' Command File.", True)
                        log.track(3, "ERROR: [" + str(e) + "]", True)

            else:
                if cfg.tracking:
                    log.track(2, "WARNING: Unknown Command Message: " + str(seg_cmd), True)
                    log.track(2, "Continue w/NEXT C&C SEGMENT.", True)

            msg_pos += 1
            continue

        #---------------------------------------------------------------
        # Handle 'Sensor' C&C Segment TYPE
        #---------------------------------------------------------------
        elif seg_type == 1:   # SENSOR
            if cfg.tracking:
                log.track(2, "Handle 'Sensor' C&C Segment TYPE.", True)
                log.track(3, "NOT IMPLEMENTED YET; Continue w/NEXT SEGMENT.", True)
            msg_pos += seg_size
            continue

        #---------------------------------------------------------------
        # Handle 'Node' C&C Segment TYPE
        #---------------------------------------------------------------
        elif seg_type == 2:   # NODE
            if cfg.tracking:
                log.track(2, "Handle 'Node' C&C Segment TYPE.", True)

            seg_fmt = ">" +str(seg_size) + "B"
            segment = msg[msg_pos:msg_pos+seg_size]
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
                fname = "proc_node_cfg_" + str(seg_indx).zfill(5) + ".json"
                ffile = str(nepi_home)+ "/proc_nodes/" + str(fname)
                seg_parsed = json.loads(segment)
                seg_dumped = json.dumps(seg_parsed, indent=4, sort_keys=False)
                success = writeFloatFile(cfg, log,3, True, ffile, str(seg_dumped))
                if success[0]:
                    sdk_action = True   # Got at least 1 C&C message for the SDK.
            except Exception as e:
                if cfg.tracking:
                    log.track(3, "Problem(s) Creating 'proc_node' File.", True)
                    log.track(3, "ERROR: [" + str(e) + "]", True)

            msg_pos += seg_size
            continue
            
        #---------------------------------------------------------------
        # Handle 'Geofence' C&C Segment TYPE
        #---------------------------------------------------------------
        elif seg_type == 3:   # GEOFENCE
            if cfg.tracking:
                log.track(2, "Handle 'Geofence' C&C Segment TYPE.", True)
                log.track(3, "NOT IMPLEMENTED YET; Continue w/NEXT SEGMENT.", True)


            #seg_fmt = ">" +str(seg_size) + "B"
            #segment = msg[msg_pos:msg_pos+seg_size]
            #seg_len = len(segment)
            #seg_hex = segment.encode('hex')

            #if cfg.tracking:
                #log.track(15, "seg_fmt: [" + str(seg_fmt) + "]", True)
                #log.track(15, "segment: [" + str(segment) + "]", True)
                #log.track(15, "seg_len: [" + str(seg_len) + "]", True)
                #log.track(15, "seg_hex: [" + str(seg_hex) + "]", True)

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

            msg_pos += seg_size
            continue

        #---------------------------------------------------------------
        # Handle 'Rule' C&C Segment TYPE
        #---------------------------------------------------------------
        elif seg_type == 4:   # RULE
            if cfg.tracking:
                log.track(2, "Handle 'Rule' C&C Segment TYPE.", True)
                log.track(3, "NOT IMPLEMENTED YET; Continue w/NEXT SEGMENT.", True)

            msg_pos += seg_size
            continue

        #---------------------------------------------------------------
        # Handle 'Trigger' C&C Segment TYPE
        #---------------------------------------------------------------
        elif seg_type == 5:   # TRIGGER
            if cfg.tracking:
                log.track(2, "Handle 'Trigger' C&C Segment TYPE.", True)
                log.track(3, "NOT IMPLEMENTED YET; Continue w/NEXT SEGMENT.", True)

            msg_pos += seg_size
            continue

        #---------------------------------------------------------------
        # Handle UNKNOWN C&C Downlink Segment TYPE
        #---------------------------------------------------------------
        else:
            if cfg.tracking:
                log.track(2, "WARNING: Handle 'Unknown' C&C Segment TYPE.", True)
                log.track(2, "No Implementation; Continue w/NEXT SEGMENT.", True)

            msg_pos += seg_size
            continue

########################################################################
# If Messages Received and on FLoat, Tell SDK.
########################################################################
  
if (cfg.state == "fl") and (sdk_action == True):
    if cfg.tracking:
        log.track(0, "Notify the SDK it has C&C Downlink Messages.", True)

    try:
        sdkproc = '/opt/numurus/ros/nepi-utilities/process-updates.sh'
        devnull = open(os.devnull, 'wb')
        Popen(['nohup', str(sdkproc)], stdout=devnull, stderr=devnull)

    except Exception as e:
        if cfg.tracking:
            log.track(1, "Error(s) Executing 'sdk' Shell.", True)
            log.track(1, "ERROR: [" + str(e) + "]", True)
            log.track(1, "Continue.", True)

########################################################################
# Application Complete: Close Comms and EXIT the Bot-Recv Subsystem.
########################################################################

if cfg.tracking:
    log.track(0, "DONE Processing Downlink Messages; Close Comms.", True)

success = bc.close(1)

if cfg.tracking:
    log.track(0, "EXIT the 'BotRecv' Subsystem.", True)

sys.exit(0)

