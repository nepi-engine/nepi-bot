########################################################################
##
##  Module: botcfg.py
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
##  Revision:   1.11 2019/06/03  16:15:00
##  Comment:    Add DB housekeeping purge supression.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
##  Revision:   1.10 2019/06/03  14:45:00
##  Comment:    Add log_clear to manage log clearing.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
##  Revision:   1.9 2019/05/05  10:45:00
##  Comment:    Refactor various 'int' declarations to 'bool' status.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
##  Revision:   1.8 2019/05/05  10:45:00
##  Comment:    Add support for 'zlib' and 'msgpack' msg management.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
##  Revision:   1.7 2019/05/01  13:55:00
##  Comment:    Add Iridium SP attempt and timeout support.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
##  Revision:   1.6 2019/05/01  09:50:00
##  Comment:    Add 'comms' keyword-value pair support.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
##  Revision:   1.5 2019/04/04  10:20:00
##  Comment:    Add 'baud' and 'tout' keyword-value pairs.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
##  Revision:   1.4 2019/04/02  16:00:00
##  Comment:    Re-write; Added sys info; simplified factory default.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
##  Revision:   1.3 2019/03/04  15:15:00
##  Comment:    Added FACTORY_TYPE (communications 'type').
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
##  Revision:   1.2 2019/02/21  12:15:00
##  Comment:    Added database management of configuration.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
##  Revision:   1.1 2019/02/04  09:56:00
##  Comment:    Module Instantiation; ported from old botlib.py.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
########################################################################

import os
import sys
import json
import platform
from botdefs import Machine, Machines
from botdefs import nepi_home, bot_cfg_file
from bothelp import readFloatFile

v_botcfg = "bot61-20190620"

########################################################################
##  Class: BotCfg (Retrieve the NEPI-Bot Configuration File).
########################################################################

class BotCfg(object):

    def __init__(self):
        self.factory = False
        self.bot_cfg_json = None
        self.enum = None
        self.emsg = None

    def initcfg(self):
        try:
            self.machine = platform.machine()
            self.platform = platform.platform(terse=True)
            self.release = platform.release()
            self.system = platform.system()
            self.version = platform.version()
            self.processor = platform.processor()
            self.python_compiler = platform.python_compiler()
            self.python_version = platform.python_version()

            self.cfgfile = bot_cfg_file
            success, self.bot_cfg_json = readFloatFile(None, None, 0, self.cfgfile, False, True)
        except Exception as e:
            self.enum = "CFG001"
            self.emsg = "ERROR: [" + str(e) + "]"
            success = [ False, self.enum, self.emsg ]

        # If reading the specified configuration file fails, construct
        # a FACTORY DEFAULT CONFIG FILE that provides enough information
        # to continue running and, eventually, notify the Cloud of the
        # problem.  We'll assume we are running on the Float in a live
        # environment, but capture a modicum of logging infomation for
        # possible analysis later. Turn off debug mode (which implies
        # console output).
        if not success[0]:
            if self.system == "Windows" or self.system == "Linux":
                self.state = "ut"
                self.debugging  = 5
                self.logging  = 5
                self.timing = True
                self.locking = False
                self.comms = True
                self.type = "Ethernet"
                self.host = "10.0.0.116"
                self.port = 7770
                self.baud = 19200
                self.tout = 1
                self.isp_open_attm = 10
                self.isp_open_tout = 1
                self.protocol = 1
                self.packet_size = 1024
                self.sys_status_file = "sys_status_file"
                self.db_dir = "db"
                self.db_name = "float.db"
                self.db_deletes = 1
                self.data_dir = "data"
                self.data_zlib = True
                self.data_msgpack = True
                self.log_dir = "log"
                self.log_clear = True
                self.br_log_name = "brlog.txt"
                self.bs_log_name = "bslog.txt"
                self.bu_log_name = "bulog.txt"
                self.wt_changed = 0
                self.pipo_scor_wt = 0.5
                self.pipo_qual_wt = 0.5
                self.pipo_size_wt = 0.5
                self.pipo_trig_wt = 0.5
                self.pipo_time_wt = 1.0
                self.purge_rating = 0.05
                self.max_msg_size = 4096
            else:
                self.state = "fl"
                self.debugging  = -1
                self.logging  = 11
                self.timing = True
                self.locking = False
                self.comms = True
                self.type = "Iridium"
                self.host = "None"
                self.port = "/dev/ttyUSB0"
                self.baud = 19200
                self.tout = 1
                self.isp_open_attm = 10
                self.isp_open_tout = 1
                self.protocol = 1
                self.packet_size = 1024
                self.sys_status_file = "sys_status_file"
                self.db_dir = "db"
                self.db_name = "float.db"
                self.db_deletes = 1
                self.data_dir = "data"
                self.data_zlib = True
                self.data_msgpack = True
                self.log_dir = "log"
                self.log_clear = True
                self.br_log_name = "brlog.txt"
                self.bs_log_name = "bslog.txt"
                self.bu_log_name = "bulog.txt"
                self.wt_changed = 0
                self.pipo_scor_wt = 0.5
                self.pipo_qual_wt = 0.5
                self.pipo_size_wt = 0.5
                self.pipo_trig_wt = 0.5
                self.pipo_time_wt = 1.0
                self.purge_rating = 0.05
                self.max_msg_size = 1020

            self.tracking = bool(self.debugging) or bool(self.logging)
            self.data_dir_path = nepi_home + "/" + self.data_dir
            self.db_file = nepi_home + "/" + self.db_dir + "/" + self.db_name
            self.br_log_file = nepi_home + "/" + self.log_dir + "/" + self.br_log_name
            self.bs_log_file = nepi_home + "/" + self.log_dir + "/" + self.bs_log_name
            self.bu_log_file = nepi_home + "/" + self.log_dir + "/" + self.bu_log_name

            self.factory = True
        else:
            # Get the Bot Configuration information into useful
            # global variables.
            self.state = str(self.bot_cfg_json["state"])
            
            self.debugging = int(self.bot_cfg_json["debugging"])
            if (int(self.debugging) < -1):
                self.debugging = -1
            if (int(self.debugging) > 23):
                self.debugging = 23

            self.logging = int(self.bot_cfg_json["logging"])
            if (int(self.logging) < -1):
                self.logging = -1
            if (int(self.logging) > 23):
                self.logging = 23

            if (int(self.logging) > -1) or (int(self.debugging) > -1):
                self.tracking = True
            else:
                self.tracking = False

            self.timing = bool(self.bot_cfg_json["timing"])
            self.locking = bool(self.bot_cfg_json["locking"]) 
            self.comms = bool(self.bot_cfg_json["comms"]) 
            self.type = str(self.bot_cfg_json["type"])
            self.host = str(self.bot_cfg_json["host"])
            self.port = str(self.bot_cfg_json["port"])
            self.baud = int(self.bot_cfg_json["baud"])
            self.tout = int(self.bot_cfg_json["tout"])
            self.isp_open_attm = int(self.bot_cfg_json["isp_open_attm"])
            self.isp_open_tout = int(self.bot_cfg_json["isp_open_tout"])
            self.protocol = int(self.bot_cfg_json["protocol"])
            self.packet_size = int(self.bot_cfg_json["packet_size"])
            self.sys_status_file = str(self.bot_cfg_json["sys_status_file"])
            self.data_dir = str(self.bot_cfg_json["data_dir"])
            self.data_dir_path = nepi_home + "/" + self.data_dir
            self.data_zlib = bool(self.bot_cfg_json["data_zlib"])
            self.data_msgpack = bool(self.bot_cfg_json["data_msgpack"])
            self.db_dir = str(self.bot_cfg_json["db_dir"])
            self.db_name = str(self.bot_cfg_json["db_name"])
            self.db_deletes = str(self.bot_cfg_json["db_deletes"])
            self.db_file = nepi_home + "/" + self.db_dir + "/" + self.db_name
            self.log_dir = str(self.bot_cfg_json["log_dir"])
            self.log_clear = str(self.bot_cfg_json["log_clear"])
            self.br_log_name = str(self.bot_cfg_json["br_log_name"])
            self.br_log_file = nepi_home + "/" + self.log_dir + "/" + self.br_log_name
            self.bs_log_name = str(self.bot_cfg_json["bs_log_name"])
            self.bs_log_file = nepi_home + "/" + self.log_dir + "/" + self.bs_log_name
            self.bu_log_name = str(self.bot_cfg_json["bu_log_name"])
            self.bu_log_file = nepi_home + "/" + self.log_dir + "/" + self.bu_log_name
            self.wt_changed = int(self.bot_cfg_json["wt_changed"])
            self.pipo_scor_wt = float(self.bot_cfg_json["pipo_scor_wt"])
            self.pipo_qual_wt = float(self.bot_cfg_json["pipo_qual_wt"])
            self.pipo_size_wt = float(self.bot_cfg_json["pipo_size_wt"])
            self.pipo_trig_wt = float(self.bot_cfg_json["pipo_trig_wt"])
            self.pipo_time_wt = float(self.bot_cfg_json["pipo_time_wt"])
            self.purge_rating = float(self.bot_cfg_json["purge_rating"])
            self.max_msg_size = int(self.bot_cfg_json["max_msg_size"])
        
