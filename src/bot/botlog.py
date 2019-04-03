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
##  Revision History
##  ----------------
##  
##  Revision:   1.8 2019/04/02  14:15:00
##  Comment:    Re-write; simplify tracking; make robust.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##  
##  Revision:   1.7 2019/03/22  14:15:00
##  Comment:    Fixed no new line in debugging titles.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##  
##  Revision:   1.6 2019/03/18  09:40:00
##  Comment:    Make sure no debugging/logging when botcfg < 0.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##  
##  Revision:   1.5 2019/02/21  09:50:00
##  Comment:    Add database configuration management.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##  
##  Revision:   1.4 2019/02/20  09:50:00
##  Comment:    Add controls for dbg/log levels 6-10.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##  
##  Revision:   1.3 2019/02/19  09:15:00
##  Comment:    Update to conform with new config options.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##  
##  Revision:   1.2 2019/02/13  09:10:00
##  Comment:    Rewrite to manage recv/send logs plus formatting.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##  
##  Revision:   1.1 2019/02/04  08:53:00
##  Comment:    Module Instantiation; ported from old botproc.py
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
########################################################################

import os
import sys
import time
import errno
from botdefs import Machines, nepi_home, bot_cfg_file, bot_db_file

########################################################################
##  A Class for creating and managing various NEPI-Bot Log Files.
########################################################################

class BotLog(object):

    def __init__(self, _cfg, _which):
        self.cfg = _cfg
        self.name = str(_which)
        self.file = None
        self.indent = "                     "

        if str(_which) == "BOT-RECV":
            self.recv = True
            self.file = self.cfg.br_log_file
        elif str(_which) == "BOT-SEND":
            self.send = True
            self.file = self.cfg.bs_log_file

    def initlog(self, _lev):
        # If debugging, we're writing to the console.
        if self.cfg.debugging:
            # Bomb if any configuration errors.
            if self.cfg.enum != None:
                sys.stdout.write(time.ctime() + ":\n")
                sys.stdout.write(str(time.ctime()) + ": FATAL CONFIGURATION ERROR: " + str(self.cfg.enum) + "\n")
                sys.stdout.write(str(time.ctime()) + ": " + str(self.cfg.emsg) + "\n")
                sys.stdout.write(time.ctime() + ":\n")
                sys.stdout.flush()
                sys.exit(1)
            else:
                sys.stdout.write(str(time.ctime()) + ": STARTING '" + str(self.name) + "' DEBUGGING:\n")
                if not self.recv and not self.send:
                    sys.stdout.write(str(time.ctime()) + ":    ERROR: LOG001: Name should be either 'BOT-RECV' or 'BOT-SEND.\n")

            sys.stdout.write(time.ctime() + ":\n")
            sys.stdout.flush()

        if self.cfg.logging:
            if not os.path.exists(os.path.dirname(self.file)):
                try:
                    os.makedirs(os.path.dirname(self.file))
                    self.log = open(self.file, 'w')
                    self.log.write(str(time.ctime() + ": STARTING " + str(self.name) + " LOGGING:\n"))
                    self.log.write(str(time.ctime() + "\n"))
                    self.log.flush()
                    self.log.close()
                except Exception as e:
                    if self.cfg.debugging:
                        self.errtrack("LOG002", "Can't Initialize Logging.")
                        self.errtrack("LOG002", str(e))
                        self.errtrack("LOG002", "Logging Turned OFF")
                        self.cfg.logging = False

        if self.cfg.tracking:
            if self.cfg.factory:
                self.track(_lev, "Could Not Open Bot Cfg File: " + str(bot_cfg_file), True)
                self.track(_lev, "Using Factory Def Cfg: " + str(self.cfg.state), True)
            else:
                self.track(_lev, "Using Bot Config File: " + str(bot_cfg_file), True)
                self.track(_lev+6, "JSON Format: " + str(self.cfg.bot_cfg_json), True)

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
            self.track(_lev+1, "locking: " + str(self.cfg.logging), True)
            self.track(_lev+1, "type: " + str(self.cfg.type), True)
            self.track(_lev+1, "host: " + str(self.cfg.host), True)
            self.track(_lev+1, "port: " + str(self.cfg.port), True)
            self.track(_lev+1, "protocol: " + str(self.cfg.protocol), True)
            self.track(_lev+1, "packet_size: " + str(self.cfg.packet_size), True)
            self.track(_lev+1, "sys_status_file: " + str(self.cfg.sys_status_file), True)
            self.track(_lev+1, "data_dir: " + str(self.cfg.data_dir), True)
            self.track(_lev+1, "data_dir_path: " + str(self.cfg.data_dir_path), True)
            self.track(_lev+1, "db_file: " + str(self.cfg.db_file), True)
            self.track(_lev+1, "log_dir: " + str(self.cfg.log_dir), True)
            self.track(_lev+1, "br_log_name: " + str(self.cfg.br_log_name), True)
            self.track(_lev+1, "br_log_file: " + str(self.cfg.br_log_file), True)
            self.track(_lev+1, "bs_log_name: " + str(self.cfg.bs_log_name), True)
            self.track(_lev+1, "bs_log_file: " + str(self.cfg.bs_log_file), True)
            self.track(_lev+1, "wt_changed: " + str(self.cfg.wt_changed), True)
            self.track(_lev+1, "pipo_scor_wt: " + str(self.cfg.pipo_trig_wt), True)
            self.track(_lev+1, "pipo_qual_wt: " + str(self.cfg.pipo_qual_wt), True)
            self.track(_lev+1, "pipo_trig_wt: " + str(self.cfg.pipo_trig_wt), True)
            self.track(_lev+1, "pipo_time_wt: " + str(self.cfg.pipo_time_wt), True)
            self.track(_lev+1, "purge_rating: " + str(self.cfg.purge_rating), True)
            self.track(_lev+1, "max_msg_size: " + str(self.cfg.max_msg_size), True)

    def track(self, lev, msg, new):
        yesdbg = True
        yeslog = True
        if lev < 0:
            lev = 0

        if lev > 11:
            lev = 11
            
        if (self.cfg.debugging < 0) or (lev > self.cfg.debugging):
            yesdbg = False
        
        if (self.cfg.logging < 0) or (lev > self.cfg.logging):
            yeslog = False

        if yesdbg or yeslog:
            inum = lev
            if lev > 5:
                inum = lev - 6

            if self.cfg.timing:
                self.ti = str(time.ctime()) + ": "
            else:
                self.ti = ""

            if new:
                self.nl = "\n"
            else:
                self.nl = ""

            self.ind = self.indent[0:3*inum]
            self.msg = str(self.ti) + str(self.ind) + str(msg) + str(self.nl)

            if yesdbg:
                sys.stdout.write(self.msg)
                sys.stdout.flush()

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
