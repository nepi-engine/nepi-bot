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
##  Revision:   1.6 2019/05/08  14:45:00
##  Comment:    Added Housekeeping Functionality.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
##  Revision:   1.5 2019/05/08  11:00:00
##  Comment:    Added Robust DB Connection Closures.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
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
#import uuid
import json
#import math
import ast
import struct
import zlib
import msgpack
import shutil
#import socket
#import sqlite3
from subprocess import Popen
from botdefs import nepi_home
from array import array
from botcfg import BotCfg
from botlog import BotLog
from botdb import BotDB
#from botmsg import BotMsg
from botcomm import BotComm
from bothelp import writeFloatFile, resetCfgValue


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

# For Test; These are base64 test-level message (base64 used for either
# Slack or email communication)  They're pasted in here and can be
# uncommented for various levels of testing.
if not cfg.comms:
    # The following 5 Iridium Messages are Good Bulk Tests from Jacob.
    # These samples reflect all message types for bot61 Bitbucket Branch.

    # This is 1) geofence, 2) proc_node, and 3) rules.
    #msg_b64 = "GFQAa15VkJ9TmZ6fF5+Zwr48NS8xKSc1hWFFWWpRSWZyavHkpsU5iSWnHcwm1idtW1e7JCc/L/30gZAeN/n5jDugcsbl8mZqOrugcsFsRxLeNizEpw8AEGEAa1uZl5+SGl9SWZC6uLggeUVmXnFJYl5yKuPa4qLkeIRcSUXKGpAITJ5hdV5qQWZ8fmlJQWkJw7KCxKLE3OIpTSvAjPjMFIalZYk5pWB9CEFGiCADQoQFIuKAEGGFiHAAACBwAGtdXlSakxqfmcK1PDUvMSknNYVxRUFRam5mcWrxpOY1xbmJRSUlRZnpQBUMKzLziksS85JTmVanFaUWxpdkFKUWZ5y23zkTBGahqmaFq2ZAVs24NjG5JDM/L74YKJSZIrgqNzMvviC1KDM/5SyfAAA="

    # This is 1) sensor, 2) trigger, and 3) schedule.
    #msg_b64 = "CCQAa1pZnJpXnF8Un5nCvKwgsSgxt3hi0wowAyjEtLQsMac0VQQAKEMAa15XnJtYVFJSlJkeX1JZkMqxIjOvuCQxLzmVbVlBYlFibvHkphVgRnxmCsPSssSc0lRGhAgTROQs+wWEGDNE7Mx/ADhBAGtdkpOZl8qzNjG5JDM/L744tTA+M4VvVXFJYlFJfElmbuq5mGgD12UFqUWZ+SlnZRTW5SZWxBelFqSWZII0/AcA"

    # This is 1) sensor, 2) geofence, and 3) action.
    #msg_b64 = "CCQAa1pZnJpXnF8Un5nCvKwgsSgxt3hi0wowAyjEtLQsMac0VQQAGFQAa15VkJ9TmZ6fF5+Zwr48NS8xKSc1hWFFWWpRSWZyavHkpsU5iSWnHcwm1idtW1e7JCc/L/30gZAeN/n5jDugcsbl8mZqOrugcsFsRxLeNizEpw8AMDkAa1qbmFySmZ8XX5xaGJ+Zwrocwi2e3LQSKpGZwrgmN7EiPqW0KBEkIIIkw4Qic5YpAkmOFVWO+QUA"

    # This is 1) geofence, 2) node, 3) rules, 4) sensor, 5) trig, 6) sched,  and 7) action.
    #msg_b64 = "GFQAa15VkJ9TmZ6fF5+Zwr48NS8xKSc1hWFFWWpRSWZyavHkpsU5iSWnHcwm1idtW1e7JCc/L/30gZAeN/n5jDugcsbl8mZqOrugcsFsRxLeNizEpw8AEGEAa1uZl5+SGl9SWZC6uLggeUVmXnFJYl5yKuPa4qLkeIRcSUXKGpAITJ5hdV5qQWZ8fmlJQWkJw7KCxKLE3OIpTSvAjPjMFIalZYk5pWB9CEFGiCADQoQFIuKAEGGFiHAAACBwAGtdXlSakxqfmcK1PDUvMSknNYVxRUFRam5mcWrxpOY1xbmJRSUlRZnpQBUMKzLziksS85JTmVanFaUWxpdkFKUWZ5y23zkTBGahqmaFq2ZAVs24NjG5JDM/L74YKJSZIrgqNzMvviC1KDM/5SyfAAAIJABrWlmcmlecXxSfmcK8rCCxKDG3eGLTCjADKMS0tCwxpzRVBAAoQwBrXlecm1hUUlKUmR5fUlmQyrEiM6+4JDEvOZVtWUFiUWJu8eSmFWBGfGYKw9KyxJzSVEaECBNE5Cz7BYQYM0TszH8AOEEAa12Sk5mXyrM2MbkkMz8vvji1MD4zhW9VcUliUUl8SWZu6rmYaAPXZQWpRZn5KWdlFNblJlbEF6UWpJZkgjT8BwAwOQBrWpuYXJKZnxdfnFoYn5nCuhzCLZ7ctBIqkZnCuCY3sSI+pbQoESQggiTDhCJzlikCSY4VVY75BQA="

    # This is 1) sensor, 2) trigger, and 3) schedule.
    #msg_b64 = "GGMAa15VkJ9TmZ6fF5+Zwr08NS8xKSc1hXFFWWpRSWZyavHkpsU5iSWnHSwP/W1b8Yd3SU5+XvrpAyE8bo46H35B5YzKdRcq/boOlQtoO5LwtmEhVM5BPfVv161dMDkDDse3D6UAOEEAa12Sk5mXyrM2MbkkMz8vvji1MD4zhW9VcUliUUl8SWZu6rmYaAPXZQWpRZn5KWdlFNblJlbEF6UWpJZkgjT8BwA="

    # This is 1) scuttle, 2) geofence, 2) node, and 3) rules.
    #msg_b64 = "AAH8ARhUAGteVZCfU5menxefmcK+PDUvMSknNYVhRVlqUUlmcmrx5KbFOYklpx3MJtYnbVtXuyQnPy/99IGQHjf5+Yw7oHLG5fJmajq7oHLBbEcS3jYsxKcPABBhAGtbmZefkhpfUlmQuri4IHlFZl5xSWJecirj2uKi5HiEXElFyhqQCEyeYXVeakFmfH5pSUFpCcOygsSixNziKU0rwIz4zBSGpWWJOaVgfQhBRoggA0KEBSLigBBhhYhwAAAgcABrXV5UmpMan5nCtTw1LzEpJzWFcUVBUWpuZnFq8aTmNcW5iUUlJUWZ6UAVDCsy84pLEvOSU5lWpxWlFsaXZBSlFmectt85EwRmoapmhatmQFbNuDYxuSQzPy++GCiUmSK4KjczL74gtSgzP+UsnwAA"

    #msg_b64 = "AAH8AQgkAGtaWZyaV5xfFJ+ZwrysILEoMbd4YtMKMAMoxLS0LDGnNFUEAChDAGteV5ybWFRSUpSZHl9SWZDKsSIzr7gkMS85lW1ZQWJRYm7x5KYVYEZ8ZgrD0rLEnNJURoQIE0TkLPsFhBgzROzMfwA4QQBrXZKTmZfKszYxuSQzPy++OLUwPjOFb1VxSWJRSXxJZm7quZhoA9dlBalFmfkpZ2UU1uUmVsQXpRaklmSCNPwHAA=="

    #msg_b64 = "AAH8AQgkAGtaWZyaV5xfFJ+ZwrysILEoMbd4YtMKMAMoxLS0LDGnNFUEABhUAGteVZCfU5menxefmcK+PDUvMSknNYVhRVlqUUlmcmrx5KbFOYklpx3MJtYnbVtXuyQnPy/99IGQHjf5+Yw7oHLG5fJmajq7oHLBbEcS3jYsxKcPADA5AGtam5hckpmfF1+cWhifmcK6HMItnty0EiqRmcK4JjexIj6ltCgRJCCCJMOEInOWKQJJjhVVjvkFAA=="

    msg_b64 = "AAH8ARhUAGteVZCfU5menxefmcK+PDUvMSknNYVhRVlqUUlmcmrx5KbFOYklpx3MJtYnbVtXuyQnPy/99IGQHjf5+Yw7oHLG5fJmajq7oHLBbEcS3jYsxKcPABBhAGtbmZefkhpfUlmQuri4IHlFZl5xSWJecirj2uKi5HiEXElFyhqQCEyeYXVeakFmfH5pSUFpCcOygsSixNziKU0rwIz4zBSGpWWJOaVgfQhBRoggA0KEBSLigBBhhYhwAAAgcABrXV5UmpMan5nCtTw1LzEpJzWFcUVBUWpuZnFq8aTmNcW5iUUlJUWZ6UAVDCsy84pLEvOSU5lWpxWlFsaXZBSlFmectt85EwRmoapmhatmQFbNuDYxuSQzPy++GCiUmSK4KjczL74gtSgzP+UsnwAACCQAa1pZnJpXnF8Un5nCvKwgsSgxt3hi0wowAyjEtLQsMac0VQQAKEMAa15XnJtYVFJSlJkeX1JZkMqxIjOvuCQxLzmVbVlBYlFibvHkphVgRnxmCsPSssSc0lRGhAgTROQs+wWEGDNE7Mx/ADhBAGtdkpOZl8qzNjG5JDM/L744tTA+M4VvVXFJYlFJfElmbuq5mGgD12UFqUWZ+SlnZRTW5SZWxBelFqSWZII0/AcAMDkAa1qbmFySmZ8XX5xaGJ+Zwrocwi2e3LQSKpGZwrgmN7EiPqW0KBEkIIIkw4Qic5YpAkmOFVWO+QUA"

    #msg_b64 = "AAH8ARhjAGteVZCfU5menxefmcK9PDUvMSknNYVxRVlqUUlmcmrx5KbFOYklpx0sD/1tW/GHd0lOfl766QMhPG6OOh9+QeWMynUXKv26DpULaDuS8LZhIVTOQT31b9etXTA5Aw7Htw+lADhBAGtdkpOZl8qzNjG5JDM/L744tTA+M4VvVXFJYlFJfElmbuq5mGgD12UFqUWZ+SlnZRTW5SZWxBelFqSWZII0/AcA"

    # This is Bot Config Changes.
    #msg_b64 = "AAH8AQCB/eteUpycX3Ta/uZMEJi1pLA0Mee0/WNjMFhSUpSZzrikJDM39bT9SYiKpQWlRelA7sbXrXI7At+syC1Ojy/OrEo9y6izpKSyIHW5Z1FmSmZp7pKM/OKS9ZkQjl5eakGmXmb+koL8opKV+impZfqhwU6GS5ISS1POqjYsK4AYwcQAAA=="

    success = [ True, None, None ]
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
        log.track(14, "msg_hex: [" + str(msg_hex) + "] <-- s/b double.", True)

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
            log.track(15, "Starting:  [" + str(int(msg_pos)) + "]", True)
            log.track(15, "seg_prot:  [" + str(int(seg_prot)) + "]", True)
            log.track(15, "seg_type:  [" + str(int(seg_type)) + "]", True)
            log.track(15, "seg_size:  [" + str(int(seg_size)) + "]", True)
            log.track(15, "seg_indx:  [" + str(int(seg_indx)) + "]", True)
            log.track(15, "seg_flag:  [" + str(int(seg_flag)) + "]", True)

        #---------------------------------------------------------------
        # Position at Data and Decompress/Unpack.
        #---------------------------------------------------------------
        msg_pos += 3    # Skip forward over the 3-byte segment 'header.'

        if cfg.tracking:
            log.track(2, "Bump Forward to Process Message Data.", True)
            log.track(15, "msg_pos:  [" + str(msg_pos) + "]", True)
            log.track(2, "Perform 'struct' Unpacking.", True)

        try:
            data_fmt = ">" +str(seg_size) + "s"
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

            if seg_type == 0 and (int(seg_flag) != 1):  # If Cmd, grab byte 1 of 
                data_unp = data_hex                     # data in its hex value.
            else:
                data_unp = struct.unpack(data_fmt, data_raw)[0]
                if cfg.tracking:
                    log.track(15, "data_unp: [" + str(data_unp) + "]", True)

                if cfg.data_zlib:   # Are we using 'zlib' compression?
                    if cfg.tracking:
                        log.track(2, "Perform 'zlib' Decompression.", True)

                    decompress = zlib.decompressobj(-zlib.MAX_WBITS)
                    data_dcmp = decompress.decompress(data_raw)
                    data_dcmp += decompress.flush()

                    if cfg.tracking:
                        log.track(15, "data_dcmp: [" + str(data_dcmp) + "]", True)
                    data_dsiz = len(data_dcmp)
                    if cfg.tracking:
                        log.track(15, "data_dsiz: [" + str(data_dsiz) + "]", True)

                    data_unp = data_dcmp

                if cfg.data_msgpack:    # Are we using 'msgpack' unpacking?
                    if cfg.tracking:
                        log.track(2, "Perform 'msgpack' Unpacking.", True)
                    data_mpak = msgpack.unpackb(data_dcmp)
                    if cfg.tracking:
                        log.track(16, "data_mpak: [" + str(data_mpak) + "]", True)
                    data_mlen = len(str(data_mpak))
                    if cfg.tracking:
                        log.track(16, "data_mlen: [" + str(data_mlen) + "]", True)

                    data_unp = data_mpak

            if cfg.tracking:
                log.track(15, "data_unp: [" + str(data_unp) + "]", True)

        except Exception as e:
            if cfg.tracking:
                log.track(3, "Problem(s) Processing Data Segment.", True)
                log.track(3, "ERROR: [" + str(e) + "]", True)
                log.track(3, "Continue w/next MESSAGE.", True)
            break

        #---------------------------------------------------------------
        # Process the Bot Config, Command, or SDK Config Messages.
        #---------------------------------------------------------------

        if int(seg_flag) == 1:  # --------------------------- BOT CONFIG
            if cfg.tracking:
                log.track(2, "Process BOT CONFIG.", True)

            for key in data_unp:
                val = data_unp[key]

                if cfg.tracking:
                    log.track(3, "Process keyword: [" + str(key) + "]", True)
                    log.track(4, "val: [" + str(val) + "]", True)

                if str(key) == "scor":
                    nkey = "pipo_scor_wt"
                elif str(key) == "qual":
                    nkey = "pipo_qual_wt"
                elif str(key) == "size":
                    nkey = "pipo_size_wt"
                elif str(key) == "trig":
                    nkey = "pipo_trig_wt"
                elif str(key) == "time":
                    nkey = "pipo_time_wt"
                elif str(key) == "msg_size":
                    nkey = "max_msg_size"
                elif str(key) == "type":
                    nkey = "type"
                elif str(key) == "host":
                    nkey = "host"
                elif str(key) == "port":
                    nkey = "port"
                elif str(key) == "baud":
                    nkey = "baud"
                elif str(key) == "p_size":
                    nkey = "packet_size"
                else:
                    if cfg.tracking:
                        log.track(3, "Unknown 'keyword' received: [" + str(key) + "]", True)
                        log.track(3, "Continue w/next KEYWORD.", True)

                    msg_pos += seg_size
                    continue

                if cfg.tracking:
                    log.track(4, "CFG: [" + str(nkey) + "]", True)

                log.track(3, "Update the Bot Config File.", True)
                resetCfgValue(cfg, log, 4, str(nkey), val)

            if cfg.tracking:
                log.track(3, "DONE with Bot Config Updates.", True)
                log.track(3, "Continue w/next SEGMENT.", True)

            msg_pos += seg_size
            continue

        elif seg_type == 0:     # --------------------------- COMMAND
            if cfg.tracking:
                log.track(2, "Process COMMAND.", True)
                log.track(3, "Identify Directory and Base Name.", True)

            fpath = "/commands/"
            cmd = int(data_unp)
            
            if cmd == 1:                      
                fname = "scuttle"                       # Scuttle
            elif cmd == 2:
                fname = "ping"                          # Ping
            else:
                if cfg.tracking:
                    log.track(3, "WARNING: Got 'Unknown' Command: [" + str(cmd) + "]", True)
                    log.track(3, "No Implementation; Continue w/next SEGMENT.", True)

                msg_pos += seg_size
                continue
        else:   # ------------------------------------------- CONFIGURATION
            if cfg.tracking:
                log.track(2, "Process CONFIGURATION.", True)
                log.track(3, "Identify Directory and Base File Name.", True)

            if seg_type == 1:                               # SENSOR
                fpath = "/cfg/sensors/"
                fname = "sensor_cfg"

            elif seg_type == 2:                             # NODE
                fpath = "/cfg/proc_nodes/"
                fname = "proc_node_cfg"
                
            elif seg_type == 3:                             # GEOFENCE
                fpath = "/cfg/geofence/"
                fname = "geofence_cfg"

            elif seg_type == 4:                             # RULE
                fpath = "/cfg/rules/"
                fname = "smarttrig_rule"

            elif seg_type == 5:                             # TRIGGER
                fpath = "/cfg/trig/"
                fname = "smarttrig_cfg"

            elif seg_type == 6:                             # ACTION
                fpath = "/cfg/action/"
                fname = "action_seq"
                
            elif seg_type == 7:                             # SCHEDULE
                fpath = "/cfg/sched/"
                fname = "task"
                
            else:
                if cfg.tracking:
                    log.track(2, "WARNING: Got 'Unknown' C&C Segment TYPE.", True)
                    log.track(2, "No Implementation; Continue w/next SEGMENT.", True)

                msg_pos += seg_size
                continue

        if cfg.tracking:
            log.track(4, "bpath: [" + str(fpath) + "]", True)
            log.track(4, "bname: [" + str(fname) + "]", True)
            log.track(3, "Construct File Path, Name, and File.", True)

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
                # This can be used if the JSON requires 'expansion.'
                #fpars = json.loads(data_unp)
                #if cfg.tracking:
                    #log.track(15, "fpars: [" + str(fpars) + "]", True)
                fdump = json.dumps(data_unp, indent=4, sort_keys=False)
                if cfg.tracking:
                    log.track(15, "fdump: [" + str(fdump) + "]", True)
            else:
                fpars = ""
                fdump = ""
        except Exception as e:
            if cfg.tracking:
                log.track(3, "Problem(s) Constructing C&C File.", True)
                log.track(3, "ERROR: [" + str(e) + "]", True)
                log.track(3, "Continue w/next SEGMENT.", True)
            
            msg_pos += seg_size
            continue

        if cfg.tracking:
            log.track(2, "Create Config File for the SDK.", True)

        success = writeFloatFile(cfg, log, 3, True, ffile, str(fdump))
        if success[0]:
            sdk_action = True   # Got at least 1 C&C message for the SDK.
        
        if cfg.tracking:
                log.track(2, "Continue w/next SEGMENT.", True)

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
# Bot-Recv and General Housekeeping.
########################################################################

if cfg.tracking:
    log.track(0, "Perform Housekeeping.", True)
    log.track(1, "DB Housekeeping.", True)

db = BotDB(cfg, log, 2)
success, dbconn = db.getconn(2)

if success[0]:
    if cfg.tracking:
        log.track(2, "Delete 'meta' Rows Below Purge Rating OR > 5 Days Old.", True)
        log.track(3, "db_deletes: " + str(cfg.db_deletes), True)

    if cfg.db_deletes:
        fivedaysago = int(time.time()) - (60 * 60 * 24 * 5)

        sql = "DELETE FROM meta WHERE pipo < '" + str(cfg.purge_rating) + "' OR timestamp < '" + str(fivedaysago) + "'"
        success = db.update(3, sql)

        if not success[0]:
            if cfg.tracking:
                log.track(3, "Well ... This is Awkward.", True)
        else:
            if cfg.tracking:
                log.track(3, "DONE.", True)

        if cfg.tracking:
            log.track(2, "Delete 'meta' Rows That Have Been Sent.", True)

        sql = "DELETE FROM meta WHERE state = '2'"
        success = db.update(3, sql)

        if not success[0]:
            if cfg.tracking:
                log.track(3, "Well ... This is Awkward.", True)
        else:
            if cfg.tracking:
                log.track(3, "DONE.", True)

        if cfg.tracking:
            log.track(2, "Delete 'status' Rows That Have Been Sent.", True)

        sql = "DELETE FROM status WHERE state = '2'"
        success = db.update(3, sql)

        if not success[0]:
            if cfg.tracking:
                log.track(3, "Well ... This is Awkward.", True)
        else:
            if cfg.tracking:
                log.track(3, "DONE.", True)
    else:
        if cfg.tracking:
            log.track(3, "DB Record Purge/Delete Supressed.", True)

else:
    if cfg.tracking:
        log.track(3, "DB Housekeeping Terminated.", True)

if cfg.tracking:
    log.track(1, "General Housekeeping.", True)
    log.track(2, "DONE.", True)

success = db.close(1)

########################################################################
# Application Complete: EXIT the Bot-Recv Subsystem.
########################################################################

if cfg.tracking:
    log.track(0, "EXIT the 'BotRecv' Subsystem.", True)

sys.exit(0)

