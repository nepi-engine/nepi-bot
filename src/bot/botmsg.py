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
from struct import *
import botdefs

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
        self.fmtmeta = ">BII3sBddBfHH"
        
        if self.cfg.tracking:
            self.log.track(_lev, "Creating BotMsg Class Object.", True)
            self.log.track(_lev+1, "_cfg:    " + str(self.cfg), True)
            self.log.track(_lev+1, "_log:    " + str(self.log), True)
            self.log.track(_lev+1, "_lev:    " + str(_lev), True)
            self.log.track(_lev+1, "^buf:    " + str(self.buf), True)
            self.log.track(_lev+1, "^len:    " + str(self.len), True)
            self.log.track(_lev+1, "fmtstat: " + str(self.fmtstat), True)
            self.log.track(_lev+1, "fmtmeta: " + str(self.fmtmeta), True)

    #-------------------------------------------------------------------
    # The 'packstat()' Class Library Method.
    #-------------------------------------------------------------------
    def packstat(self, _lev, _rec):
        if self.cfg.tracking:
            self.log.track(_lev, "Entering 'packstat()' Class Method.", True)
            self.log.track(_lev+1, "_lev: " + str(_lev), True)
            self.log.track(_lev+7, "_rec: " + str(_rec), True)

        self.tmp = None
        try:
            #-----------------------------------------------------------
            # useful calcs
            if self.cfg.tracking:
                self.log.track(_lev+1, "Compute useful date values.", True)

            utcnow          = int(time.time())
            datnow          = datetime.datetime.now()
            datnow_yr       = int(datnow.year)
            startdate       = datetime.datetime(int(datnow_yr), 1, 1)
            utcstart        = int(time.mktime(startdate.timetuple()))

            if self.cfg.tracking:
                self.log.track(_lev+8, "utcnow:          " + str(utcnow), True)
                self.log.track(_lev+8, "datnow:          " + str(datnow), True)
                self.log.track(_lev+8, "datnow_yr:       " + str(datnow_yr), True)
                self.log.track(_lev+8, "startdate:       " + str(startdate), True)
                self.log.track(_lev+8, "utcstart:        " + str(utcstart), True)
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
                self.log.track(_lev+8, "segid:          " + str(segid), True)
                self.log.track(_lev+8, "statid_raw:     " + str(statid_raw), True)
                self.log.track(_lev+8, "statid_adj:     " + str(statid_adj), True)
                self.log.track(_lev+8, "heading_raw:    " + str(heading_raw), True)
                self.log.track(_lev+8, "heading_deg:    " + str(heading_deg), True)
                self.log.track(_lev+8, "heading_adj:    " + str(heading_adj), True)
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
            longi_adj       = int(math.floor(longi_mic))        # s/b <= 29 bits

            trig_wk_ct      = int(_rec[17])                     # trig_wake_count
            trig_wk_adj     = int(trig_wk_ct % 1024)            # s/b <= 10 bits

            pack2b64 = (int(tstamp_adj)<<39) + (int(longi_sgn)<<38) + (int(longi_adj)<<10) + (int(trig_wk_adj))

            if self.cfg.tracking:
                self.log.track(_lev+8, "tstamp_ms:      " + str(tstamp_ms), True)
                self.log.track(_lev+8, "tstamp:         " + str(tstamp), True)
                self.log.track(_lev+8, "tstamp_adj:     " + str(tstamp_adj), True)
                self.log.track(_lev+8, "longi_raw:      " + str(longi_raw), True)
                self.log.track(_lev+8, "longi_mic:      " + str(longi_mic), True)
                self.log.track(_lev+8, "longi_adj:      " + str(longi_adj), True)
                self.log.track(_lev+8, "trig_wk_ct:     " + str(trig_wk_ct), True)
                self.log.track(_lev+8, "trig_wk_adj:    " + str(trig_wk_adj), True)

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
            bus_dec         = float(bus_raw * 10.0)             # decivolts in float
            bus_adj         = int(math.floor(bus_dec))          # s/b <= 5 bits

            temp_raw        = float(_rec[16])                   # temperature raw
            temp_adj        = int(math.floor(temp_raw))         # s/b <= 7 bits

            node_raw        = int(_rec[24])                     # node_cfg_index raw
            node_adj        = int(node_raw % 64)                # s/b <= 6 bits

            geo_raw         = int(_rec[25])                     # geofence_cfg_idx raw
            geo_adj         = int(geo_raw % 64)                 # s/b <= 6 bits

            pack3b32 = (int(batt_adj)<<25) + (int(bus_bit)<<20) + (int(bus_adj)<<19) + (int(temp_adj)<<12) + (int(node_adj)<<6) + (int(geo_adj))

            if self.cfg.tracking:
                self.log.track(_lev+2, "batt_adj(b):    " + str(bin(batt_adj)), True)
                self.log.track(_lev+2, "bus_adj(b):     " + str(bin(bus_adj)), True)
                self.log.track(_lev+2, "temp_adj(b):    " + str(bin(temp_adj)), True)
                self.log.track(_lev+2, "node_adj(b):    " + str(bin(node_adj)), True)
                self.log.track(_lev+2, "geo_adj(b):     " + str(bin(geo_adj)), True)

                self.log.track(_lev+2, "pack3b32(bin):  " + str(bin(pack3b32)[2:].zfill(32)), True)
                self.log.track(_lev+2, "pack2b32(int):  " + str(int(pack3b32)), True)
                self.log.track(_lev+2, "pack3b32(hex):  " + str(hex(int(pack3b32))[2:].zfill(8)), True)
                self.log.track(_lev+1, "Bitpack bits 128-159 (bytes 16-19).", True)


            #-----------------------------------------------------------
            # Bytes 16-19 (pack4b32)

            wake_raw        = int(_rec[18])         # wake_event_id (0-255)
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
                self.log.track(_lev+2, "wake_adj(b):    " + str(bin(wake_adj)), True)
                self.log.track(_lev+2, "task_adj(b):    " + str(bin(task_adj)), True)
                self.log.track(_lev+2, "trig_adj(b):    " + str(bin(trig_adj)), True)
                self.log.track(_lev+2, "rule_adj(b):    " + str(bin(rule_adj)), True)
                self.log.track(_lev+2, "sens_adj(b):    " + str(bin(sens_adj)), True)

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

            lati_raw        = int(_rec[11])                     # latitide (-90.0-90.0)
            if lati_raw < 0.0:
                lati_sgn    = 1                                 # sign bit is 1
                lati_mic    = float(lati_raw * -1000000.0)      # latitude microdegrees
            else:
                lati_sgn    = 0                                 # sign bit is 0
                lati_mic    = float(lati_raw * 1000000.0)       # latitude microdegrees
            lati_adj        = int(math.floor(lati_mic))         # s/b <= 28 bits

            pack6b32 = (int(swinc_adj)<<28) + (int(lati_sgn)<<27) + int(lati_adj)

            if self.cfg.tracking:
                self.log.track(_lev+2, "swinc_adj(b):   " + str(bin(swinc_adj)), True)
                self.log.track(_lev+2, "lati_sgn(b):    " + str(bin(lati_sgn)), True)
                self.log.track(_lev+2, "lati_adj(b):    " + str(bin(lati_adj)), True)

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
                self.log.track(_lev+2, "navsat_adj(b):  " + str(bin(navsat_adj)), True)

                self.log.track(_lev+2, "pack7b24(bin):  " + str(bin(pack7b24)[2:].zfill(32)), True)
                self.log.track(_lev+2, "pack7b24(int):  " + str(int(pack7b24)), True)
                self.log.track(_lev+2, "pack7b24(hex):  " + str(hex(int(pack7b24))[2:].zfill(8)), True)
                self.log.track(_lev+1, "Combine Packing Increments.", True)

            self.tmp = pack(self.fmtstat, pack1b32, pack2b64, pack3b32, pack4b32, pack5b32, pack6b32, pack7b24)

            if self.cfg.tracking:
                self.log.track(_lev+2, "length: " + str(len(self.tmp)), True)
                self.log.track(_lev+2, "hexbuf: " + str(self.tmp.encode("hex")), True)

        except Exception as e:
            enum = "MSG101"
            emsg = "packstat(): [" + str(e)  + "]"
            if self.cfg.tracking:
                self.log.track(_lev, str(enum) + ": " + str(emsg), True)
            return [ False, str(enum), str(emsg) ]

        if self.len + len(self.tmp) > self.cfg.max_msg_size:
            enum = "MSG102"
            emsg = "packstat(): INSUFFICIENT ROOM to pack Status Record."
            if self.cfg.tracking:
                self.log.track(_lev, str(enum) + ": " + str(emsg), True)
            return [ False, str(enum), str(emsg) ]

        self.buf += self.tmp
        self.len += len(self.tmp)

        if self.cfg.tracking:
            self.log.track(_lev+1, "Status Record PACKED.", True)
            self.log.track(_lev+2, "^len: " + str(len(self.buf)), True)
            self.log.track(_lev+2, "^buf: " + str(self.buf.encode("hex")), True)

        return [ True, None, None ]

    #-------------------------------------------------------------------
    # The 'packmeta()' Class Library Method.
    #-------------------------------------------------------------------
    def packmeta(self, _lev, _rec):
        if self.cfg.tracking:
            self.log.track(_lev, "Entering 'packmeta()' Class Method.", True)
            self.log.track(_lev+1, "_lev: " + str(_lev), True)
            self.log.track(_lev+7, "_rec: " + str(_rec), True)

        self.tmp = ""
        self.std = ""
        self.chg = ""

        std_len = int(_rec[16])
        chg_len = int(_rec[17])

        if chg_len <= 0:
            chg_bit = 0
        else:
            chg_bit = 1



        try:
            if self.cfg.tracking:
                self.log.track(_lev+1, "Access 'std' and 'chg' file sizes.", True)
            stdlen = int(_rec[16])
            chglen = int(_rec[17])
            if self.cfg.tracking:
                self.log.track(_lev+2, "stdlen: " + str(stdlen), True)
                self.log.track(_lev+2, "chglen: " + str(chglen), True)
        except Exception as e:
            enum = "MSG114"
            emsg = "packmeta(): [" + str(e)  + "]"
            if self.cfg.tracking:
                self.log.track(_lev, str(enum) + ": " + str(emsg), True)
            return [ False, str(enum), str(emsg) ]
    
        try:
            if self.cfg.tracking:
                self.log.track(_lev, "Pack the temporary Buffer.", True)
                self.log.track(_lev+1, "header:  " + str(botdefs.Messages.META), True)
                self.log.track(_lev+1, "rowid:   " + str(_rec[0]), True)
                self.log.track(_lev+1, "statID:  " + str(_rec[3]), True)
                self.log.track(_lev+1, "type:    " + str(_rec[7]), True)
                self.log.track(_lev+1, "instan:  " + str(_rec[8]), True)
                self.log.track(_lev+1, "timest:  " + str(_rec[9]), True)
                self.log.track(_lev+1, "heading: " + str(_rec[10]), True)

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
                self.log.track(_lev+8, "segid:          " + str(segid), True)
                self.log.track(_lev+8, "statid_raw:     " + str(statid_raw), True)
                self.log.track(_lev+8, "statid_adj:     " + str(statid_adj), True)
                self.log.track(_lev+8, "heading_raw:    " + str(heading_raw), True)
                self.log.track(_lev+8, "heading_deg:    " + str(heading_deg), True)
                self.log.track(_lev+8, "heading_adj:    " + str(heading_adj), True)
                self.log.track(_lev+2, "pack1b32(bin):  " + str(bin(pack1b32)[2:].zfill(32)), True)
                self.log.track(_lev+2, "pack1b32(int):  " + str(int(pack1b32)), True)
                self.log.track(_lev+2, "pack1b32(hex):  " + str(hex(int(pack1b32))[2:].zfill(8)), True)
                self.log.track(_lev+1, "Bitpack bits 32-63 (bytes 4-7).", True)
            segid           = int(botdefs.Messages.META)
            rowid           = int(_rec[0])
            heading_raw     = float(_rec[13])                   # heading raw
            heading_deg     = float(heading_raw * 100.0)        # heading centidegrees
            heading_adj     = int(math.floor(heading_deg))      # s/b <= 16 bits


            self.fmtmeta = ">BII3sBddBfHH"
            self.tmp = pack(self.fmtmeta, botdefs.Messages.META,
                        int(_rec[0]),       # rowid ('meta' Table)
                        int(_rec[3]),       # status id (FK to 'status' Table)
                        str(_rec[7]),       # type
                        int(_rec[8]),       # instance
                        _rec[9],            # timestamp
                        float(_rec[10]),    # heading
                        int(_rec[11]),      # quality
                        float(_rec[12]),    # score
                        int(stdlen),        # stdsize
                        int(chglen))        # chgsize

            metasize = len(self.tmp)
        except Exception as e:
            enum = "MSG115"
            emsg = "packmeta(): [" + str(e)  + "]"
            if self.cfg.tracking:
                self.log.track(_lev, str(enum) + ": " + str(emsg), True)
            return [ False, str(enum), str(emsg) ]

        if stdlen > 0:
            self.tmp += str(_rec[19]).encode("hex")
        if chglen > 0:
            self.tmp += str(_rec[20]).encode("hex")

        datalen = len(self.tmp)
        if self.len + datalen > self.cfg.max_msg_size:
            enum = "MSG103"
            emsg = "packmeta(): No room left to pack Meta Record."
            if self.cfg.tracking:
                self.log.track(_lev, str(enum) + ": " + str(emsg), True)
                self.log.track(_lev+1, "metasize: " + str(metasize), True)
                self.log.track(_lev+1, "stdsize:  " + str(stdlen), True)
                self.log.track(_lev+1, "chgsize:  " + str(chglen), True)
                self.log.track(_lev+1, "datalen:  " + str(datalen), True)

            return [ False, str(enum), str(emsg) ]

        self.buf += self.tmp
        self.len += len(self.tmp)

        if self.cfg.tracking:
            self.log.track(_lev+1, "^tmp: " + self.tmp.encode("hex"), True)
            self.log.track(_lev+1, "^buf: " + self.buf.encode("hex"), True)
            self.log.track(_lev+1, "^len: " + str(len(self.buf)), True)
        
        return [ True, None, None ]

