########################################################################
##
##  Module: botlog.py
##  -----------------
##
##  (c) Copyright 2019 by Numurus LLC
##
##  This document, and all information therein, is the property of
##  Numurus LLC.  It is confidential and must not be made public or
##  copied in any form.  It is loaned subject to return upon demand
##  and is not to be used directly or indirectly in any way detrimental
##  to our interests.
##
########################################################################

import os
import sys
import time
import errno
from botcfg import v_botcfg
from botcomm import v_botcomm
from botdb import v_botdb
from botdefs import Machines, nepi_home, bot_cfg_file, bot_db_file, v_botdefs
from bothelp import v_bothelp
from botmsg import v_botmsg
from botpipo import v_botpipo

v_botlog = "bot61-20190620"

########################################################################
##  A Class for creating and managing various NEPI-Bot Log Files.
########################################################################

class BotLog(object):

    def __init__(self, _cfg, _which, _version):
        self.cfg = _cfg
        self.name = str(_which)
        self.verison = str(_version)
        self.file = None
        self.app = None
        self.indent = "                          "

        if str(_which) == "BOT-RECV":
            self.file = self.cfg.br_log_file
            self.app = "botrecv"
        elif str(_which) == "BOT-SEND":
            self.file = self.cfg.bs_log_file
            self.app = "botsend"
        else:
            self.file = self.cfg.bu_log_file
            self.name = "UNKNOWN"
            self.app = "unknown"

    def initlog(self, _lev):
        # If debugging, we're writing to the console.
        if self.cfg.debugging > -1:
            # Save whatever logging is and turn off temporarily.
            logsav = self.cfg.logging
            self.cfg.logging = -1

            # Bomb if any configuration errors.
            if self.cfg.enum != None:
                self.track(0, "", True)
                self.track(0, "FATAL CONFIG ERR: " + str(self.cfg.enum), True)
                self.track(0, "FATAL CONFIG ERR: " + str(self.cfg.emsg), True)
                self.track(0, "", True)
                sys.exit(6)
            else:
                self.track(0, "STARTING '" + str(self.name) + "' DEBUGGING:", True)
                if self.name == "UNKNOWN":
                    enum = "LOG001"
                    emsg = "initlog(): Name should be either 'BOT-RECV' or 'BOT-SEND.'"
                    self.track(1, str(enum) + ": " + str(emsg), True)
                self.track(0, "", True)
                
            self.cfg.logging = logsav   # Return logging to whatever it was.

        # If logging, we're writing to a file on SD Card.
        if self.cfg.logging > -1:
            # Remove existing file.
            if self.cfg.log_clear:
                if os.path.exists(self.file):
                    try:
                        os.remove(self.file)
                    except:
                        pass

                if not os.path.exists(os.path.dirname(self.file)):
                    try:
                        os.makedirs(os.path.dirname(self.file))
                    except Exception as e:
                        self.cfg.logging = -1   # Turn off Logging
                        enum = "LOG002"
                        emsg = "initlog(): Can't Make Path: [" + str(e) + "]: Logging Turned OFF."
                        if self.cfg.tracking:
                            self.track(_lev+2, str(enum) + ": " + str(emsg), True)
                        return [ False, str(enum), str(emsg) ]

            # Save whatever debugging is and turn off temporarily.
            dbgsav = self.cfg.debugging
            self.cfg.debugging = -1
            
            if self.cfg.logging > -1:
                self.track(0, "STARTING '" + str(self.name) + "' LOGGING:", True)
                if self.name == "UNKNOWN":
                    enum = "LOG002"
                    emsg = "initlog(): Name should be either 'BOT-RECV' or 'BOT-SEND.'"
                    self.track(1, str(enum) + ": " + str(emsg), True)
                self.track(0, "", True)

            self.cfg.debugging = dbgsav   # Return debugging to whatever it was.

        if (self.cfg.debugging > -1) or (self.cfg.logging > -1):
            self.cfg.tracking = True
        else:
            self.cfg.tracking = False

        if self.cfg.tracking:
            self.track(_lev, "Using Bot Software Versions:" , True)
            self.track(_lev+1, "botcfg:  " + str(v_botcfg) , True)
            self.track(_lev+1, "botcomm: " + str(v_botcomm) , True)
            self.track(_lev+1, "botdb:   " + str(v_botdb) , True)
            self.track(_lev+1, "botdefs: " + str(v_botdefs) , True)
            self.track(_lev+1, "bothelp: " + str(v_bothelp) , True)
            self.track(_lev+1, "botlog:  " + str(v_botlog) , True)
            self.track(_lev+1, "botmsg:  " + str(v_botmsg) , True)
            self.track(_lev+1, "botpipo: " + str(v_botpipo) , True)
            self.track(_lev+1, self.app + ": " + str(self.verison) , True)

            if self.cfg.factory:
                self.track(_lev, "Could Not Open Bot Cfg File: " + str(bot_cfg_file), True)
                self.track(_lev, "Using Factory Def Cfg: " + str(self.cfg.state), True)
            else:
                self.track(_lev, "Using Bot Config File: " + str(bot_cfg_file), True)
                self.track(_lev+13, "JSON Format: " + str(self.cfg.bot_cfg_json), True)

            self.track(_lev+1, "machine: " + str(self.cfg.machine), True)
            self.track(_lev+1, "platform: " + str(self.cfg.platform), True)
            self.track(_lev+1, "release: " + str(self.cfg.release), True)
            self.track(_lev+1, "system: " + str(self.cfg.system), True)
            self.track(_lev+1, "version: " + str(self.cfg.version), True)
            self.track(_lev+1, "processor: " + str(self.cfg.machine), True)
            self.track(_lev+1, "python_compiler: " + str(self.cfg.python_compiler), True)
            self.track(_lev+1, "python_version: " + str(self.cfg.python_version), True)

            self.track(_lev+1, "state: " + str(self.cfg.state), True)
            self.track(_lev+1, "debugging: " + str(self.cfg.debugging), True)
            self.track(_lev+1, "logging: " + str(self.cfg.logging), True)
            self.track(_lev+1, "tracking: " + str(self.cfg.tracking), True)
            self.track(_lev+1, "timing: " + str(self.cfg.timing), True)
            self.track(_lev+1, "locking: " + str(self.cfg.locking), True)
            self.track(_lev+1, "comms: " + str(self.cfg.comms), True)
            self.track(_lev+1, "type: " + str(self.cfg.type), True)
            self.track(_lev+1, "host: " + str(self.cfg.host), True)
            self.track(_lev+1, "port: " + str(self.cfg.port), True)
            self.track(_lev+1, "baud: " + str(self.cfg.baud), True)
            self.track(_lev+1, "isp_open_attm: " + str(self.cfg.isp_open_attm), True)
            self.track(_lev+1, "isp_open_tout: " + str(self.cfg.isp_open_tout), True)
            self.track(_lev+1, "protocol: " + str(self.cfg.protocol), True)
            self.track(_lev+1, "packet_size: " + str(self.cfg.packet_size), True)
            self.track(_lev+1, "sys_status_file: " + str(self.cfg.sys_status_file), True)
            self.track(_lev+1, "data_dir: " + str(self.cfg.data_dir), True)
            self.track(_lev+1, "data_dir_path: " + str(self.cfg.data_dir_path), True)
            self.track(_lev+1, "data_zlib: " + str(self.cfg.data_zlib), True)
            self.track(_lev+1, "data_msgpack: " + str(self.cfg.data_msgpack), True)
            self.track(_lev+1, "db_file: " + str(self.cfg.db_file), True)
            self.track(_lev+1, "db_name: " + str(self.cfg.db_name), True)
            self.track(_lev+1, "db_deletes: " + str(self.cfg.db_deletes), True)
            self.track(_lev+1, "log_dir: " + str(self.cfg.log_dir), True)
            self.track(_lev+1, "log_clear: " + str(self.cfg.log_clear), True)
            self.track(_lev+1, "br_log_name: " + str(self.cfg.br_log_name), True)
            self.track(_lev+1, "br_log_file: " + str(self.cfg.br_log_file), True)
            self.track(_lev+1, "bs_log_name: " + str(self.cfg.bs_log_name), True)
            self.track(_lev+1, "bs_log_file: " + str(self.cfg.bs_log_file), True)
            self.track(_lev+1, "bu_log_name: " + str(self.cfg.bu_log_name), True)
            self.track(_lev+1, "bu_log_file: " + str(self.cfg.bu_log_file), True)
            self.track(_lev+1, "wt_changed: " + str(self.cfg.wt_changed), True)
            self.track(_lev+1, "pipo_scor_wt: " + str(self.cfg.pipo_trig_wt), True)
            self.track(_lev+1, "pipo_qual_wt: " + str(self.cfg.pipo_qual_wt), True)
            self.track(_lev+1, "pipo_trig_wt: " + str(self.cfg.pipo_trig_wt), True)
            self.track(_lev+1, "pipo_size_wt: " + str(self.cfg.pipo_size_wt), True)
            self.track(_lev+1, "pipo_time_wt: " + str(self.cfg.pipo_time_wt), True)
            self.track(_lev+1, "purge_rating: " + str(self.cfg.purge_rating), True)
            self.track(_lev+1, "max_msg_size: " + str(self.cfg.max_msg_size), True)

    def track(self, lev, msg, new):
        yesdbg = True
        yeslog = True
        if (lev < 0) or (lev > 23): # Completely outside loggin/debugging bounds.
            return

        if (self.cfg.debugging < 0) or (lev > self.cfg.debugging):  # Ignore DBG
            yesdbg = False
        
        if (self.cfg.logging < 0) or (lev > self.cfg.logging):      # Ignore LOG
            yeslog = False

        if yesdbg or yeslog:
            inum = lev
            if lev > 11:
                inum = lev - 12

            if self.cfg.timing:
                self.ti = str(time.ctime()) + ": "
            else:
                self.ti = ""

            if new:
                self.nl = "\n"
            else:
                self.nl = ""

            self.ind = self.indent[0:2*inum]
            self.msg = str(self.ti) + str(self.ind) + str(msg) + str(self.nl)

            if yesdbg:
                try:
                    sys.stdout.write(self.msg)
                    sys.stdout.flush()
                except:
                    self.cfg.debugging = False   # On error, turn off debugging

            if yeslog:
                try:
                    self.log = open(self.file, 'a')
                    self.log.write(self.msg)
                    self.log.flush()
                    self.log.close()
                except:
                    self.cfg.logging = False   # On error, turn off logging

    def errtrack(self, _enum, _emsg):
        msg = "ERROR: " + str(_enum) + ": " + str(_emsg)
        self.track(True, msg, True)

    def reset(self, _lev):
        self.initlog(_lev)
