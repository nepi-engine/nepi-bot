########################################################################
##
##  Module: botmsg.py
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
##  Revision:   1.1 2019/02/25  10:50:00
##  Comment:    Module Instantiation.
##  Developer:  
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
########################################################################

import os
import math
import time
from struct import *

########################################################################
##  The Float's Message Class Library
########################################################################

class BotMsg(object):
    #-------------------------------------------------------------------
    # Initiallize the BotMess Class Library Object.
    #-------------------------------------------------------------------
    def __init__(self, _cfg, _log, _lev, _typ):
        self.cfg = _cfg
        self.log = _log
        self.typ = _typ
        if self.typ == "STATUS":
            self.fmthead = ">BId11s10s"
            self.fmtbody = ">IdddddBBBHBBBBBBBBH"
        else:
            self.fmthead = ">BId11s10s"
            self.fmtbody = ">WhoReallyKnows"

        self.buf = ""
        self.len = 0
        if self.cfg.tracking:
            self.log.track(_lev, "Creating BotMess Class Object.", True)
            self.log.track(_lev+1, "_lev: " + str(_lev), True)
            self.log.track(_lev+1, "_cfg: " + str(self.cfg), True)
            self.log.track(_lev+1, "_log: " + str(self.log), True)
            self.log.track(_lev+1, "_typ: " + str(self.typ), True)

    #-------------------------------------------------------------------
    # The 'packhead()' Class Library Method.
    #-------------------------------------------------------------------
    def packhead(self, _lev, _ser, _rev):
        if self.cfg.tracking:
            self.log.track(_lev, "Entering 'packhead()' Class Method.", True)
            self.log.track(_lev+1, "_lev: " + str(_lev), True)
            self.log.track(_lev+1, "_ser: " + str(_ser), True)
            self.log.track(_lev+1, "_rev: " + str(_rev), True)
            self.log.track(_lev+1, "type: " + str(self.typ), True)
            self.log.track(_lev+1, "head: " + str(self.fmthead), True)

        try:
            ser = str(bytearray(_ser, "UTF-8"))
            rev = str(bytearray(_rev, "UTF-8"))
            if self.cfg.tracking:
                self.log.track(_lev+1, "ser:  " + str(ser), True)
                self.log.track(_lev+1, "rev:  " + str(rev), True)

            hdr = pack(self.fmthead, int(3), int(0), int(time.time()), ser, rev) 
            if self.cfg.tracking:
                self.log.track(_lev+1, "hdr:  " + hdr.encode("hex"), True)
            
            return [ True, None, None ], hdr
        except Exception as e:
            enum = "MSG101"
            emsg = "packhead(): [" + str(e)  + "]"
            if self.cfg.tracking:
                self.log.errtrack(str(enum), str(emsg))
            return [ False, str(enum), str(emsg) ], None

    #-------------------------------------------------------------------
    # The 'packstat()' Class Library Method.
    #-------------------------------------------------------------------
    def packstat(self, _lev, _statjson):
        if self.cfg.tracking:
            self.log.track(_lev, "Entering 'packstat()' Class Method.", True)
            self.log.track(_lev+1, "_lev: " + str(_lev), True)
            self.log.track(_lev+7, "_statjson: " + str(_statjson), True)
            self.log.track(_lev+1, "type: " + str(self.typ), True)
            self.log.track(_lev+1, "body: " + str(self.fmtbody), True)

        try:
            buf = pack(self.fmtbody, int(_statjson["rowid"]),
                            float(_statjson["timestamp"]),
                            float(_statjson["navsat_fix_time"]),
                            float(_statjson["latitude"]),
                            float(_statjson["longitude"]),
                            float(_statjson["heading"]),
                            int(math.floor(float(_statjson["batt_charge"]))),
                            int(math.floor(float(_statjson["bus_voltage"]))),
                            int(math.floor(float(_statjson["temperature"]))),
                            int(_statjson["trig_wake_count"]),
                            int(_statjson["wake_event_type"]),
                            int(_statjson["wake_event_id"]),
                            int(_statjson["task_index"]),
                            int(_statjson["trig_cfg_index"]),
                            int(_statjson["rule_cfg_index"]),
                            int(_statjson["sensor_cfg_index"]),
                            int(_statjson["node_cfg_index"]),
                            int(_statjson["geofence_cfg_index"]),
                            int(_statjson["state_flags"]))

            if self.cfg.tracking:
                self.log.track(_lev+1, "buf:  " + buf.encode("hex"), True)
            
            return [ True, None, None ], buf
        except Exception as e:
            enum = "MSG102"
            emsg = "packstat(): [" + str(e)  + "]"
            if self.cfg.tracking:
                self.log.errtrack(str(enum), str(emsg))
            return [ False, str(enum), str(emsg) ], None

