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
##  Revision:   1.4 2019/06/24  10:00:00
##  Comment:    Add new 'int16' Data Type for Alex.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
##  Revision:   1.3 2019/04/16  07:00:00
##  Comment:    Major Revision; Total DATA Msg Format Change.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
##  Revision:   1.2 2019/03/12  10:00:00
##  Comment:    Major Revision; Total Status Msg Format Change.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
##  Revision:   1.1 2019/02/25  10:50:00
##  Comment:    Module Instantiation.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
########################################################################

import os
import math
import time
import datetime
from datetime import date
import calendar
from struct import *
import ctypes
import botdefs

v_botmsg = "bot61-20190624"

########################################################################
##  The Float's Message Class Library
########################################################################

class BotMsg(object):
    #-------------------------------------------------------------------
    # Initiallize the BotMsg Class Library Object.
    #-------------------------------------------------------------------
    def __init__(self, _cfg, _log, _lev):
        self.cfg = _cfg
        self.log = _log
        self.buf = ""
        self.len = 0
        self.fmtstat = ">IQIIIII"
        
        if self.cfg.tracking:
            self.log.track(_lev, "Creating BotMsg Class Object.", True)
            self.log.track(_lev+1, "_cfg:    " + str(self.cfg), True)
            self.log.track(_lev+1, "_log:    " + str(self.log), True)
            self.log.track(_lev+1, "_lev:    " + str(_lev), True)
            self.log.track(_lev+1, "^buf:    " + str(self.buf), True)
            self.log.track(_lev+1, "^len:    " + str(self.len), True)
            self.log.track(_lev+1, "fmtstat: " + str(self.fmtstat), True)

    #-------------------------------------------------------------------
    # The 'packstat()' Class Library Method.
    #-------------------------------------------------------------------
    def packstat(self, _lev, _rec):
        if self.cfg.tracking:
            self.log.track(_lev, "Entering 'packstat()' Class Method.", True)
            self.log.track(_lev+1, "_lev: " + str(_lev), True)
            self.log.track(_lev+13, "_rec: " + str(_rec), True)

        try:
            #-----------------------------------------------------------
            # useful calcs
            if self.cfg.tracking:
                self.log.track(_lev+1, "Compute useful date values.", True)

            utcnow          = int(time.time())
            datnow          = datetime.datetime.now()
            datnow_yr       = int(datnow.year)
            startdate       = datetime.datetime(int(datnow_yr), 1, 1)
            utcstart        = int(calendar.timegm(startdate.timetuple()))

            if self.cfg.tracking:
                self.log.track(_lev+14, "utcnow:          " + str(utcnow), True)
                self.log.track(_lev+14, "datnow:          " + str(datnow), True)
                self.log.track(_lev+14, "datnow_yr:       " + str(datnow_yr), True)
                self.log.track(_lev+14, "startdate:       " + str(startdate), True)
                self.log.track(_lev+14, "utcstart:        " + str(utcstart), True)
                self.log.track(_lev+1, "Bitpack bits 0-31 (bytes 0-3).", True)

            #-----------------------------------------------------------
            # Bytes 0-3 (pack1b32)
            segid           = int(botdefs.Messages.STATUS)      # s/b 3 bits (0-7)

            statid_raw      = int(_rec[0])                      # Status Rec Rowid
            statid_adj      = int(statid_raw % 2**13)           # Rollover

            heading_raw     = float(_rec[13])                   # heading raw
            heading_deg     = float(heading_raw * 100.0)        # heading centidegrees
            heading_adj     = int(math.floor(heading_deg))      # s/b <= 16 bits

            pack1b32 = (int(segid) << 29) + (int(statid_adj) << 16) + int(heading_adj)

            if self.cfg.tracking:
                self.log.track(_lev+14, "segid:          " + str(segid), True)
                self.log.track(_lev+14, "statid_raw:     " + str(statid_raw), True)
                self.log.track(_lev+14, "statid_adj:     " + str(statid_adj), True)
                self.log.track(_lev+14, "heading_raw:    " + str(heading_raw), True)
                self.log.track(_lev+14, "heading_deg:    " + str(heading_deg), True)
                self.log.track(_lev+14, "heading_adj:    " + str(heading_adj), True)
                self.log.track(_lev+2, "pack1b32(bin):  " + str(bin(pack1b32)[2:].zfill(32)), True)
                self.log.track(_lev+2, "pack1b32(int):  " + str(int(pack1b32)), True)
                self.log.track(_lev+2, "pack1b32(hex):  " + str(hex(int(pack1b32))[2:].zfill(8)), True)
                self.log.track(_lev+1, "Bitpack bits 32-63 (bytes 4-7).", True)

            #-----------------------------------------------------------
            # Bytes 4-11 (pack2b64)
            tstamp_ms       = float(_rec[6])                    # timestamp in ms
            tstamp          = int(math.floor(tstamp_ms))        # strip off milliseconds
            tstamp_adj      = int(tstamp - utcstart)            # s/b <= 25 bits

            longi_raw       = float(_rec[12])                   # longitude raw
            if longi_raw < 0.0:
                longi_sgn   = 1                                 # sign bit is 1
                longi_mic   = float(longi_raw * -1000000.0)     # longitude microdegrees
            else:
                longi_sgn   = 0                                 # sign bit is 0
                longi_mic   = float(longi_raw * 1000000.0)      # longitude microdegrees
            longi_adj       = int(math.floor(longi_mic))        # s/b <= 28 bits

            trig_wk_ct      = int(_rec[17])                     # trig_wake_count
            trig_wk_adj     = int(trig_wk_ct % 1024)            # s/b <= 10 bits

            pack2b64 = (int(tstamp_adj)<<39) + (int(longi_sgn)<<38) + (int(longi_adj)<<10) + (int(trig_wk_adj))

            if self.cfg.tracking:
                self.log.track(_lev+14, "tstamp_ms:      " + str(tstamp_ms), True)
                self.log.track(_lev+14, "tstamp:         " + str(tstamp), True)
                self.log.track(_lev+14, "tstamp_adj:     " + str(tstamp_adj), True)
                self.log.track(_lev+14, "longi_raw:      " + str(longi_raw), True)
                self.log.track(_lev+14, "longi_mic:      " + str(longi_mic), True)
                self.log.track(_lev+14, "longi_adj:      " + str(longi_adj), True)
                self.log.track(_lev+14, "trig_wk_ct:     " + str(trig_wk_ct), True)
                self.log.track(_lev+14, "trig_wk_adj:    " + str(trig_wk_adj), True)

                self.log.track(_lev+2, "tstamp_adj(b):  " + str(bin(tstamp_adj)), True)
                self.log.track(_lev+2, "longi_adj(b):   " + str(bin(longi_adj)), True)
                self.log.track(_lev+2, "trig_adj(b):    " + str(bin(trig_wk_adj)), True)

                self.log.track(_lev+2, "pack2b64(bin):  " + str(bin(pack2b64)[2:].zfill(64)), True)
                self.log.track(_lev+2, "pack2b64(int):  " + str(int(pack2b64)), True)
                self.log.track(_lev+2, "pack2b64(hex):  " + str(hex(int(pack2b64))[2:].zfill(16)), True)
                self.log.track(_lev+1, "Bitpack bits 64-127 (bytes 8-15).", True)

            #-----------------------------------------------------------
            # Bytes 12-15 (pack3b32)

            batt_raw        = float(_rec[14])                   # batt_charge raw
            batt_adj        = int(math.floor(batt_raw))         # s/b <= 7 bits

            bus_raw         = float(_rec[15])                   # batt_charge raw
            if bus_raw < 0.0:
                bus_bit = 1                                     # sign bit is neg
                bus_raw = bus_raw * -1.0                        # get rid of sign
            else:
                bus_bit = 0                                     # sign bit is pos
            bus_dec         = float(bus_raw)                    # decivolts no * by 10
            bus_adj         = int(math.floor(bus_dec))          # s/b <= 5 bits

            temp_raw        = float(_rec[16])                   # temperature raw
            temp_adj        = int(math.floor(temp_raw))         # s/b <= 7 bits

            node_raw        = int(_rec[24])                     # node_cfg_index raw
            node_adj        = int(node_raw % 64)                # s/b <= 6 bits

            geo_raw         = int(_rec[25])                     # geofence_cfg_idx raw
            geo_adj         = int(geo_raw % 64)                 # s/b <= 6 bits

            pack3b32 = (int(batt_adj)<<25) + (int(bus_bit)<<20) + (int(bus_adj)<<19) + (int(temp_adj)<<12) + (int(node_adj)<<6) + (int(geo_adj))

            if self.cfg.tracking:
                self.log.track(_lev+14, "batt_adj(b):    " + str(bin(batt_adj)), True)
                self.log.track(_lev+14, "bus_adj(b):     " + str(bin(bus_adj)), True)
                self.log.track(_lev+14, "temp_adj(b):    " + str(bin(temp_adj)), True)
                self.log.track(_lev+14, "node_adj(b):    " + str(bin(node_adj)), True)
                self.log.track(_lev+14, "geo_adj(b):     " + str(bin(geo_adj)), True)

                self.log.track(_lev+2, "pack3b32(bin):  " + str(bin(pack3b32)[2:].zfill(32)), True)
                self.log.track(_lev+2, "pack2b32(int):  " + str(int(pack3b32)), True)
                self.log.track(_lev+2, "pack3b32(hex):  " + str(hex(int(pack3b32))[2:].zfill(8)), True)
                self.log.track(_lev+1, "Bitpack bits 128-159 (bytes 16-19).", True)

            #-----------------------------------------------------------
            # Bytes 16-19 (pack4b32)

            wake_raw        = int(_rec[19])         # wake_event_id (0-255)
            wake_adj        = wake_raw % 256

            task_raw        = int(_rec[20])         # task_index (0-63)
            task_adj        = task_raw % 64

            trig_raw        = int(_rec[21])         # trig_cfg_index (0-63)
            trig_adj        = trig_raw % 64

            rule_raw        = int(_rec[22])         # rule_cfg_index (0-63)
            rule_adj        = rule_raw % 63

            sens_raw        = int(_rec[23])         # sensor_cfg_index (0-63)
            sens_adj        = sens_raw % 63

            pack4b32 = (int(wake_adj)<<24) + (int(task_adj)<<18) + (int(trig_adj)<<12) + (int(rule_adj)<<6) + int(sens_adj)

            if self.cfg.tracking:
                self.log.track(_lev+14, "wake_adj(b):    " + str(bin(wake_adj)), True)
                self.log.track(_lev+14, "task_adj(b):    " + str(bin(task_adj)), True)
                self.log.track(_lev+14, "trig_adj(b):    " + str(bin(trig_adj)), True)
                self.log.track(_lev+14, "rule_adj(b):    " + str(bin(rule_adj)), True)
                self.log.track(_lev+14, "sens_adj(b):    " + str(bin(sens_adj)), True)

                self.log.track(_lev+2, "pack4b32(bin):  " + str(bin(pack4b32)[2:].zfill(32)), True)
                self.log.track(_lev+2, "pack4b32(int):  " + str(int(pack4b32)), True)
                self.log.track(_lev+2, "pack4b32(hex):  " + str(hex(int(pack4b32))[2:].zfill(8)), True)
                self.log.track(_lev+1, "Bitpack bits 160-191 (bytes 20-23).", True)


            #-----------------------------------------------------------
            # Bytes 20-23 (pack5b32)

            pack5b32 = int(_rec[26])                # state_flags

            if self.cfg.tracking:
                self.log.track(_lev+2, "pack5b32(bin):  " + str(bin(pack5b32)[2:].zfill(32)), True)
                self.log.track(_lev+2, "pack5b32(int):  " + str(int(pack5b32)), True)
                self.log.track(_lev+2, "pack5b32(hex):  " + str(hex(int(pack5b32))[2:].zfill(8)), True)
                self.log.track(_lev+1, "Bitpack bits 192-223 (bytes 24-27).", True)

            #-----------------------------------------------------------
            # bytes 24-27 (pack6b32)

            swinc_raw       = int(_rec[9])                      # sw_rev_incr (0-15)
            swinc_adj       = swinc_raw % 16                    # s/b <= 4 bits

            lati_raw        = float(_rec[11])                   # latitide (-90.0-90.0)
            if lati_raw < 0.0:
                lati_sgn    = 1                                 # sign bit is 1
                lati_mic    = float(lati_raw * -1000000.0)      # latitude microdegrees
            else:
                lati_sgn    = 0                                 # sign bit is 0
                lati_mic    = float(lati_raw * 1000000.0)       # latitude microdegrees
            lati_adj        = int(math.floor(lati_mic))         # s/b <= 27 bits

            pack6b32 = (int(swinc_adj)<<28) + (int(lati_sgn)<<27) + int(lati_adj)

            if self.cfg.tracking:
                self.log.track(_lev+14, "swinc_adj(b):   " + str(bin(swinc_adj)), True)
                self.log.track(_lev+14, "lati_sgn(b):    " + str(bin(lati_sgn)), True)
                self.log.track(_lev+14, "lati_adj(b):    " + str(bin(lati_adj)), True)

                self.log.track(_lev+2, "pack6b32(bin):  " + str(bin(pack6b32)[2:].zfill(32)), True)
                self.log.track(_lev+2, "pack6b32(int):  " + str(int(pack6b32)), True)
                self.log.track(_lev+2, "pack6b32(hex):  " + str(hex(int(pack6b32))[2:].zfill(8)), True)
                self.log.track(_lev+1, "Bitpack bits 224-255 (bytes 28-31).", True)


            #-----------------------------------------------------------
            # Bytes 28-30 (pack7b32)

            navsat_raw      = float(_rec[10])               # navsat_fix_time
            navsat_sec      = int(math.floor(navsat_raw))   # strip ms; now int
            navsat_tmp      = tstamp - navsat_sec           # navsat diff from ts
            navsat_adj      = navsat_tmp % (60*60*48)       # s/b <= 18 bits

            evnt_raw        = int(_rec[18])
            evnt_adj        = evnt_raw % 4

            pack7b24 = int(navsat_adj<<3) + int(evnt_adj)

            if self.cfg.tracking:
                self.log.track(_lev+14, "navsat_adj(b):  " + str(bin(navsat_adj)), True)
                self.log.track(_lev+14, "evnt_adj(b):    " + str(bin(evnt_adj)), True)

                self.log.track(_lev+2, "pack7b24(bin):  " + str(bin(pack7b24)[2:].zfill(32)), True)
                self.log.track(_lev+2, "pack7b24(int):  " + str(int(pack7b24)), True)
                self.log.track(_lev+2, "pack7b24(hex):  " + str(hex(int(pack7b24))[2:].zfill(8)), True)
                self.log.track(_lev+1, "Combine Packing Increments.", True)

            buf1 = pack(self.fmtstat, pack1b32, pack2b64, pack3b32, pack4b32, pack5b32, pack6b32, pack7b24)

        except Exception as e:
            enum = "MSG101"
            emsg = "packstat(): [" + str(e)  + "]"
            if self.cfg.tracking:
                self.log.track(_lev, str(enum) + ": " + str(emsg), True)
            return [ False, str(enum), str(emsg) ]

        #---------------------------------------------------------------
        # Make sure we have room for this Status Record. 
        #---------------------------------------------------------------

        if self.len + len(buf1) > self.cfg.max_msg_size:
            enum = "MSG102"
            emsg = "packstat(): INSUFFICIENT ROOM to pack Status Record."
            if self.cfg.tracking:
                self.log.track(_lev, str(enum) + ": " + str(emsg), True)
                self.log.track(_lev+1, "Max Msg Size: " + str(self.cfg.max_msg_size), True)
                self.log.track(_lev+1, "Cur Msg Size: " + str(self.len), True)
                self.log.track(_lev+1, "DP Msg Size:  " + str(len(str(buf1))), True)

            return [ False, str(enum), str(emsg) ]

        self.buf += str(buf1)
        self.len += len(self.buf)

        if self.cfg.tracking:
            self.log.track(_lev+1, "Status Record PACKED.", True)
            self.log.track(_lev+2, "^len: " + str(self.len), True)
            self.log.track(_lev+2, "^buf: " + str(self.buf.encode("hex")), True)

        return [ True, None, None ]


    #-------------------------------------------------------------------
    # The 'packmeta()' Class Library Method.
    #-------------------------------------------------------------------
    def packmeta(self, _lev, _rec, _tim, _idx):
        if self.cfg.tracking:
            self.log.track(_lev, "Entering 'packmeta()' Class Method.", True)
            self.log.track(_lev+1, "_lev:     " + str(_lev), True)
            self.log.track(_lev+13, "_rec:     " + str(_rec), True)
            self.log.track(_lev+1, "_tim:     " + str(_tim), True)
            self.log.track(_lev+1, "_idx:     " + str(_idx), True)
            #self.log.track(_lev+1, "Insure Segment Will Fit.", True)

        self.len = len(self.buf)
        chg_file     = str(_rec[20])
        chg_eligible = int(_rec[21])

        if self.cfg.tracking:
            self.log.track(_lev+1, "^len:     " + str(self.len), True)
            self.log.track(_lev+1, "^buf:     " + str(self.buf.encode("hex")), True)
            self.log.track(_lev+1, "chg_file: " + str(chg_file), True)
            self.log.track(_lev+1, "eligible: " + str(chg_eligible), True)

        #self.tmp = ""

        #std_len      = int(_rec[16])
        #chg_len      = int(_rec[17])

        #if chg_eligible:
            #seg_len = chg_len
        #else:
            #seg_len = std_len

        #if self.cfg.tracking:
            #self.log.track(_lev+2, "maxsize:  " + str(self.cfg.max_msg_size), True)
            #self.log.track(_lev+2, "cursize:  " + str(self.len), True)
            #self.log.track(_lev+2, "stdsize:  " + str(std_len), True)
            #self.log.track(_lev+2, "chgsize:  " + str(chg_len), True)
            #self.log.track(_lev+2, "chg_file: " + str(chg_file), True)
            #self.log.track(_lev+2, "eligible: " + str(chg_eligible), True)
            #self.log.track(_lev+2, "seg_len:  " + str(seg_len), True)

        #if (self.len + seg_len) > self.cfg.max_msg_size:
            #enum = "MSG103"
            #emsg = "packmeta(): No room left to pack this Meta Record."
            #if self.cfg.tracking:
                #self.log.track(_lev, str(enum) + ": " + str(emsg), True)

            #return [ False, str(enum), str(emsg) ]


        #---------------------------------------------------------------
        # Pack the Segment's Common Header (bytes 0-3) Pt 1.
        #---------------------------------------------------------------
        if self.cfg.tracking:
            self.log.track(_lev+1, "Pack Common Header (bits 0-31; bytes 0-3).", True)

        try:
            pack1b32 = 0

            segid = int(botdefs.Messages.META)                # s/b 3 bits (0-7)

            stime_ms        = float(_tim)                       # stat timestamp in ms
            stime           = int(math.floor(stime_ms))         # strip off milliseconds
            mtime_ms        = float(_rec[9])                    # meta timestamp in ms
            mtime           = int(math.floor(mtime_ms))         # strip off milliseconds
            mtime_dif       = stime - mtime
            if mtime_dif < 0.0:                                 # BEFORE Status Time?
                mtime_sgn   = 1                                 # sign bit is 1
                mtime_dif   = mtime_dif * -1
            else:                                               # AFTER Status Time?
                mtime_sgn   = 0                                 # sign bit is 0
            dp_timestamp    = mtime_dif % 512                   # rollover to 10 bits

            dp_node_type    = str(_rec[7])
            dp_node_code    = str(_rec[33])                     # The actual "code"
            dp_instance     = str(_rec[8])
            dp_index        = _idx
            data_kind       = 0                                 # Default is 'std'
            data_change     = 0                                 # Default is 'ignored'

            if chg_eligible:                                    # If 'chg' is Eligibile
                if (str(chg_file) == "NULL") or (str(chg_file) == "Null") or (str(chg_file) == "null"):
                    data_kind   = 0                             # Revert to 'std'
                    data_change = 0                             # Revert to 'ignored'
                    chg_eligible = 0                            # Something was wrong
                elif str(chg_file) == "NC":
                    data_kind   = 1                             # Process a 'chg'
                    data_change = 0                             # No Change from Previous
                else:
                    data_kind   = 1                             # Process a 'chg'
                    data_change = 1                             # Yes Change from Previous
        
        except Exception as e:
            enum = "MSG112"
            emsg = "packmeta(): Error Calculating 1b32: " + str(e)
            if self.cfg.tracking:
                self.log.track(_lev+2, str(enum) + ": " + str(emsg), True)

            return [ False, str(enum), str(emsg) ]

        if self.cfg.tracking:
            self.log.track(_lev+14, "segid:         " + str(segid), True)
            self.log.track(_lev+14, "mtime_dif:     " + str(mtime_dif), True)
            self.log.track(_lev+14, "mtime_sgn:     " + str(mtime_sgn), True)
            self.log.track(_lev+14, "dp_timestamp:  " + str(dp_timestamp), True)
            self.log.track(_lev+14, "dp_node_type:  " + str(dp_node_type), True)
            self.log.track(_lev+14, "dp_node_code:  " + str(dp_node_code), True)
            self.log.track(_lev+14, "dp_instance:   " + str(dp_instance), True)
            self.log.track(_lev+14, "dp_index:      " + str(dp_index), True)
            self.log.track(_lev+14, "data_kind:     " + str(data_kind), True)
            self.log.track(_lev+14, "data_change:   " + str(data_change), True)

        try:
            pack1b32 = (int(segid) << 29) + (int(mtime_sgn) << 28) + (int(dp_timestamp) << 19) + (int(dp_node_code) << 13) + (int(dp_instance) << 9) + (int(dp_index) << 2) + (int(data_kind) << 1) + int(data_change)
        except Exception as e:
            enum = "MSG103"
            emsg = "packmeta(): Error Packing 1b32: " + str(e)
            if self.cfg.tracking:
                self.log.track(_lev+2, str(enum) + ": " + str(emsg), True)

            return [ False, str(enum), str(emsg) ]

        if self.cfg.tracking:
            self.log.track(_lev+2, "pack1b32(bin): " + str(bin(pack1b32)[2:].zfill(32)), True)
            self.log.track(_lev+2, "pack1b32(int): " + str(int(pack1b32)), True)
            self.log.track(_lev+2, "pack1b32(hex): " + str(hex(int(pack1b32))[2:].zfill(8)), True)
            self.log.track(_lev+1, "Bitpack bits 32-95 (bytes 4-11).", True)

        #---------------------------------------------------------------
        # Pack the Segment's Common Header (bytes 4-11) Pt 2.
        #---------------------------------------------------------------

        try:
            statid_raw      = int(_rec[3])                      # Status Rec Rowid
            statid_adj      = int(statid_raw % 2**12)           # s/b <= 12 bits
            #statid_adj      = 0

            heading_raw     = float(_rec[10])                   # heading raw
            heading_deg     = float(heading_raw * 100.0)        # heading centidegrees
            heading_adj     = int(math.floor(heading_deg))      # s/b <= 16 bits

            pitch_raw       = int(float(_rec[22]))              # pitch raw
            if pitch_raw < 0:
                pitch_sgn   = 1                                 # pitch sign bit (-)
                pitch_adj   = pitch_raw * -1                    # s/b <= 5 bits
            else:
                pitch_sgn   = 0                                 # pitch sign bit (+)
                pitch_adj   = pitch_raw                         # s/b <= 5 bits

            roll_raw        = int(float(_rec[23]))              # roll raw
            if roll_raw < 0:
                roll_sgn    = 1                                 # roll sign bit (-)
                roll_adj    = roll_raw * -1                     # s/b <= 5 bits
            else:
                roll_sgn    = 0                                 # roll sign bit (+)
                roll_adj    = pitch_raw                         # s/b <= 5 bits

            if chg_eligible:                                    # if sending 'chg' DP
                channels_adj    = int(_rec[30])                 # channels good as is
                dt_raw          = str(_rec[31])                 # data type raw
                if dt_raw == "uint8":
                    dt_adj      = 0
                elif dt_raw == "int32":
                    dt_adj      = 1
                elif dt_raw == "int16":
                    dt_adj      = 3
                else:
                    dt_adj      = 2

                size_adj        = int(_rec[17])                 # size good as is
            else:
                channels_adj    = int(_rec[24])                 # channels good as is
                dt_raw          = str(_rec[28])                 # data type good as is
                if dt_raw == "uint8":
                    dt_fmt      = ">B"
                    dt_byt      = 1
                    dt_adj      = 0
                elif dt_raw == "int16":
                    dt_fmt      = ">h"
                    dt_byt      = 2
                    dt_adj      = 3
                elif dt_raw == "int32":
                    dt_fmt      = ">i"
                    dt_byt      = 4
                    dt_adj      = 1
                else:
                    dt_fmt      = ">f"
                    dt_byt      = 4
                    dt_adj      = 2

                size_adj        = int(_rec[16])                 # size good as is

        except Exception as e:
            enum = "MSG113"
            emsg = "packmeta(): Error Calculating 2b64: " + str(e)
            if self.cfg.tracking:
                self.log.track(_lev+2, str(enum) + ": " + str(emsg), True)

            return [ False, str(enum), str(emsg) ]

        if self.cfg.tracking:
            self.log.track(_lev+2,  "segid:        " + str(segid), True)
            self.log.track(_lev+14, "statid_raw:   " + str(statid_raw), True)
            self.log.track(_lev+2,  "statid_adj:   " + str(statid_adj), True)
            self.log.track(_lev+14, "heading_raw:  " + str(heading_raw), True)
            self.log.track(_lev+14, "heading_deg:  " + str(heading_deg), True)
            self.log.track(_lev+2,  "heading_adj:  " + str(heading_adj), True)
            self.log.track(_lev+14, "pitch_raw:    " + str(pitch_raw), True)
            self.log.track(_lev+2,  "pitch_sgn:    " + str(pitch_sgn), True)
            self.log.track(_lev+2,  "pitch_adj:    " + str(pitch_adj), True)
            self.log.track(_lev+14, "roll_raw:     " + str(roll_raw), True)
            self.log.track(_lev+2,  "roll_sgn:     " + str(roll_sgn), True)
            self.log.track(_lev+2,  "roll_adj:     " + str(roll_adj), True)
            self.log.track(_lev+2,  "channels_adj: " + str(channels_adj), True)
            self.log.track(_lev+2,  "dt_raw:       " + str(dt_raw), True)
            self.log.track(_lev+2,  "dt_fmt:       " + str(dt_fmt), True)
            self.log.track(_lev+2,  "dt_byt:       " + str(dt_byt), True)
            self.log.track(_lev+2,  "dt_adj:       " + str(dt_adj), True)
            self.log.track(_lev+2,  "size_adj:     " + str(size_adj), True)

        try:
            pack2b64 = 0
            pack2b64 = (int(statid_adj) << 52) + (int(heading_adj) << 36) + (int(pitch_sgn) << 35) + (int(pitch_adj) << 30) + (int(roll_sgn) << 29) + (int(roll_adj) << 24) + (int(channels_adj) << 20) + (int(dt_adj) << 16) + (int(size_adj) << 4)
        except Exception as e:
            enum = "MSG114"
            emsg = "packmeta(): Error Packing 2b64: " + str(e)
            if self.cfg.tracking:
                self.log.track(_lev+2, str(enum) + ": " + str(emsg), True)

            return [ False, str(enum), str(emsg) ]

        if self.cfg.tracking:
            self.log.track(_lev+2, "pack2b64(bin):  " + str(bin(pack2b64)[2:].zfill(64)), True)
            self.log.track(_lev+2, "pack2b64(int):  " + str(int(pack2b64)), True)
            self.log.track(_lev+2, "pack2b64(hex):  " + str(hex(int(pack2b64))[2:].zfill(16)), True)

        #---------------------------------------------------------------
        # Pack the Segment's 'std' or 'chg' Data (bytes 12+)
        #---------------------------------------------------------------

        if not chg_eligible:
            if self.cfg.tracking:
                self.log.track(_lev+1, "Bitpack 'std' bits 96-127 (bytes 12-15).", True)

            try:
                samples_adj     = int(_rec[27])                     # Samples good as is
                rows_adj        = int(_rec[25])                     # Rows good as is
                cols_adj        = int(_rec[26])                     # Cols good as is
            except Exception as e:
                enum = "MSG123"
                emsg = "packmeta(): Error Calculating 3b32: " + str(e)
                if self.cfg.tracking:
                    self.log.track(_lev+2, str(enum) + ": " + str(emsg), True)

                return [ False, str(enum), str(emsg) ]

            if self.cfg.tracking:
                self.log.track(_lev+2,  "samples_adj: " + str(samples_adj), True)
                self.log.track(_lev+2,  "rows_adj:    " + str(rows_adj), True)
                self.log.track(_lev+2,  "cols_adj:    " + str(cols_adj), True)

            try:
                pack3b32 = 0
                pack3b32 = (int(samples_adj) << 22) + (int(rows_adj) << 11) + int(cols_adj)
            except Exception as e:
                enum = "MSG114"
                emsg = "packmeta(): Error Packing 3b32: " + str(e)
                if self.cfg.tracking:
                    self.log.track(_lev+2, str(enum) + ": " + str(emsg), True)

                return [ False, str(enum), str(emsg) ]

            if self.cfg.tracking:
                self.log.track(_lev+2, "pack3b32(bin):  " + str(bin(pack3b32)[2:].zfill(32)), True)
                self.log.track(_lev+2, "pack3b32(int):  " + str(int(pack3b32)), True)
                self.log.track(_lev+2, "pack3b32(hex):  " + str(hex(int(pack3b32))[2:].zfill(8)), True)
                self.log.track(_lev+1, "Pack 'std' Data Product.", True)

            #-----------------------------------------------------------
            # Pack 'std' Data Product.
            #-----------------------------------------------------------

            if self.cfg.tracking:
                self.log.track(_lev+1, "Put 'std' Segment Together.", True)

            try:
                siz1 = 16
                std1=ctypes.create_string_buffer(siz1)

                if self.cfg.tracking:
                    self.log.track(_lev+2, "Segment Header.", True)
                    self.log.track(_lev+3, "std1 Size:   " + str(siz1), True)
                    self.log.track(_lev+15, "std1 Info:   " + str(std1), True)

                std1 = pack(str(">IQI"), pack1b32, pack2b64, pack3b32)

                if self.cfg.tracking:
                    self.log.track(_lev+3, "std1(len):  " + str(len(str(std1))), True)
                    self.log.track(_lev+15, "std1(hex):  " + str(std1).encode("hex"), True)

                lst2 = _rec[29].strip('[').strip(']').strip(' ').split(',')
                len2 = len(lst2)

                if self.cfg.tracking:
                    self.log.track(_lev+2, "Segment Data.", True)
                    self.log.track(_lev+15, "std2 List:   " + str(lst2), True)
                    self.log.track(_lev+15, "std2 Items:  " + str(int(len2)), True)
                    self.log.track(_lev+15, "std2 Type:   " + str(dt_fmt), True)
                    self.log.track(_lev+15, "std2 Format: " + str(dt_raw), True)
                    self.log.track(_lev+15, "std2 Bytes:  " + str(int(dt_byt)), True)

                std2 = ""
                for i in range(0, len2):
                    if self.cfg.tracking:
                        self.log.track(_lev+14, "Pack Item #" + str(i) + "  val: " + str(lst2[i]), True)

                    if str(dt_fmt) == ">f":
                        tmp3 = pack(str(dt_fmt), float(lst2[i]))
                    else:
                        tmp3 = pack(str(dt_fmt), int(lst2[i]))

                    std2 = str(std2) + tmp3

                std3 = str(std1) + str(std2)

            except Exception as e:
                enum = "MSG141"
                emsg = "packmeta(): [" + str(e)  + "]"
                if self.cfg.tracking:
                    self.log.track(_lev, str(enum) + ": " + str(emsg), True)
                return [ False, str(enum), str(emsg) ]

            if self.cfg.tracking:
                self.log.track(_lev+2, "std3(len):  " + str(len(str(std3))), True)
                self.log.track(_lev+2, "std3(hex):  " + str(std3).encode("hex"), True)
                self.log.track(_lev+1, "Load Segment into the Uplink Message.", True)
 
        else:
            if self.cfg.tracking:
                self.log.track(_lev+1, "Bitpack 'chg' DATA bits 96+ (bytes 12+).", True)
                self.log.track(_lev+1, "NOT IMPLEMENTED YET.", True)


        #---------------------------------------------------------------
        # Again, make sure we have room for this Data Product. 
        #---------------------------------------------------------------

        chklen = self.len + len(str(std3))
        if chklen > self.cfg.max_msg_size:
            enum = "MSG199"
            emsg = "packmeta(): INSUFFICIENT ROOM to pack this Data Product."
            if self.cfg.tracking:
                self.log.track(_lev, str(enum) + ": " + str(emsg), True)
                self.log.track(_lev+1, "Max Msg Size: " + str(self.cfg.max_msg_size), True)
                self.log.track(_lev+1, "Cur Msg Size: " + str(self.len), True)
                self.log.track(_lev+1, "DP Msg Size:  " + str(len(str(std3))), True)

            return [ False, str(enum), str(emsg) ]

        self.buf += str(std3)
        self.len += len(self.buf)

        if self.cfg.tracking:
            self.log.track(_lev+2, "^len now: " + str(len(self.buf)), True)
            self.log.track(_lev+2, "^buf now: " + self.buf.encode("hex"), True)
        
        return [ True, None, None ]

