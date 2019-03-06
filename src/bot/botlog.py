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

    def __init__(self, cfg, lev, recv):
        self.cfg = cfg
        self.lev = lev
        self.recv = recv
        self.send = not recv
        self.indent = "                     "

        if self.recv:
            self.recvfile = cfg.br_log_file
            self.recvname = "BOT-RECV"
        elif self.send:
            self.sendfile = cfg.bs_log_file
            self.sendname = "BOT-SEND"

    def initlog(self):
        if self.cfg.debugging:
            if self.recv:
                self.name = self.recvname
            else:
                self.name = self.sendname

            sys.stdout.write(str(time.ctime() + ": STARTING " + str(self.name) + " DEBUGGING:"))
            sys.stdout.write(time.ctime() + ":")
            sys.stdout.flush()

            #print(str(time.ctime() + ": STARTING " + str(self.name) + " DEBUGGING:"))
            #print(str(time.ctime() + ":"))

        if self.cfg.logging:
            if self.recv:
                if not os.path.exists(os.path.dirname(self.cfg.br_log_file)):
                    try:
                        os.makedirs(os.path.dirname(self.cfg.br_log_file))
                    except OSError as exc: # Guard against race condition and file errors
                        self.recv = False

                if self.recv:
                    try:
                        self.log = open(self.cfg.br_log_file, 'w')
                        self.log.write(str(time.ctime() + ": STARTING THE " +          self.recvname + " LOG FILE.\n"))
                        self.log.flush()
                        self.log.close()
                        self.cfg.recv = True
                    except:
                        self.cfg.recv = False

            if self.send:
                if not os.path.exists(os.path.dirname(self.cfg.bs_log_file)):
                    try:
                        os.makedirs(os.path.dirname(self.cfg.bs_log_file))
                    except OSError as exc: # Guard against race condition and file errors
                        self.send = False

                if self.send:
                    try:
                        os.makedirs(os.path.dirname(self.cfg.bs_log_file))
                    except OSError as exc: # Guard against race condition and file errors
                        if exc.errno != errno.EEXIST:
                            raise
                    finally:
                        try:
                            self.log = open(self.cfg.bs_log_file, 'w')
                            self.log.write(str(time.ctime() + ": STARTING THE " + self.sendname + " LOG FILE.\n"))
                            self.log.flush()
                            self.log.close()
                            self.cfg.send = True
                        except:
                            self.cfg.send = False

        if self.cfg.tracking:
            if not self.cfg.nodb:
                self.track(self.lev, "Using Float DB for Config:" + str(bot_db_file), True)
            elif self.cfg.factory:
                self.track(self.lev, "Can't Open Bot Cfg File: " + str(bot_cfg_file), True)
                self.track(self.lev, "Using Factory Def Cfg: " + str(self.cfg.machine), True)
            else:
                self.track(self.lev, "Using Bot Config File: " + str(bot_cfg_file), True)
                self.track(self.lev+7, "JSON Format: " + str(self.cfg.bot_cfg_json), True)

            self.track(self.lev+1, "machine: " + str(self.cfg.machine), True)
            self.track(self.lev+1, "debugging: " + str(self.cfg.debugging), True)
            self.track(self.lev+1, "logging: " + str(self.cfg.logging), True)
            self.track(self.lev+1, "tracking: " + str(self.cfg.tracking), True)
            self.track(self.lev+1, "timing: " + str(self.cfg.timing), True)
            self.track(self.lev+1, "locking: " + str(self.cfg.logging), True)
            self.track(self.lev+1, "type: " + str(self.cfg.type), True)
            self.track(self.lev+1, "host: " + str(self.cfg.host), True)
            self.track(self.lev+1, "port: " + str(self.cfg.port), True)
            self.track(self.lev+1, "protocol: " + str(self.cfg.protocol), True)
            self.track(self.lev+1, "packet_size: " + str(self.cfg.packet_size), True)
            self.track(self.lev+1, "sys_status_file: " + str(self.cfg.sys_status_file), True)
            self.track(self.lev+1, "data_dir: " + str(self.cfg.data_dir), True)
            self.track(self.lev+1, "data_dir_path: " + str(self.cfg.data_dir_path), True)
            self.track(self.lev+1, "db_file: " + str(self.cfg.db_file), True)
            self.track(self.lev+1, "log_dir: " + str(self.cfg.log_dir), True)
            self.track(self.lev+1, "br_log_name: " + str(self.cfg.br_log_name), True)
            self.track(self.lev+1, "br_log_file: " + str(self.cfg.br_log_file), True)
            self.track(self.lev+1, "bs_log_name: " + str(self.cfg.bs_log_name), True)
            self.track(self.lev+1, "bs_log_file: " + str(self.cfg.bs_log_file), True)
            self.track(self.lev+1, "wt_changed: " + str(self.cfg.wt_changed), True)
            self.track(self.lev+1, "pipo_scor_wt: " + str(self.cfg.pipo_trig_wt), True)
            self.track(self.lev+1, "pipo_qual_wt: " + str(self.cfg.pipo_qual_wt), True)
            self.track(self.lev+1, "pipo_trig_wt: " + str(self.cfg.pipo_trig_wt), True)
            self.track(self.lev+1, "pipo_time_wt: " + str(self.cfg.pipo_time_wt), True)
            self.track(self.lev+1, "purge_rating: " + str(self.cfg.purge_rating), True)
            self.track(self.lev+1, "max_msg_size: " + str(self.cfg.max_msg_size), True)

    def track(self, lev, msg, new):
        yesdbg = True
        yeslog = True
        if lev < 0 or lev > self.cfg.debugging:
            yesdbg = False
        
        if lev < 0 or lev > self.cfg.logging:
            yeslog = False

        if yesdbg or yeslog:
            inum = lev
            if lev > 5:
                inum = 11 - lev

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
                if self.recv:
                    try:
                        self.log = open(self.recvfile, 'a')
                        self.log.write(self.msg)
                        self.log.flush()
                        self.log.close()
                    except:
                        self.recv = False   # On error, turn off recv logging

                if self.send:
                    try:
                        self.log = open(self.sendfile, 'a')
                        self.log.write(self.msg)
                        self.log.flush()
                        self.log.close()
                    except:
                        self.recv = False   # On error, turn off recv logging

    def errtrack(self, errnum, msg):
        self.msg = "\n**ERROR " + str(errnum) + ": " + str(msg)
        self.track(0, self.msg, True)

    def reset(self):
        self.initlog()
