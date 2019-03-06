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
import json
from botdefs import Machine, Machines
from botdefs import nepi_home, bot_cfg_file, bot_db_file
import bothelp
import sqlite3

########################################################################
##  Class: BotCfg (Retrieve the NEPI-Bot Configuration File).
########################################################################

class BotCfg(object):

    def __init__(self):
        self.nodb = True
        self.factory = False
        self.bot_cfg_json = None
        try:
            if os.path.isfile(bot_db_file):
                dbc = sqlite3.connect(bot_db_file)
                cursor = dbc.cursor()
                cursor.execute("""SELECT * FROM admin""")
                row = cursor.fetchone()
                self.machine = int(row[0])
                self.debugging = int(row[1])
                self.logging = int(row[2])
                self.tracking = int(row[3])
                self.timing = int(row[4])
                self.locking = int(row[5])
                self.type = str(row[6])
                self.host = str(row[7])
                self.port = int(row[8])
                self.protocol = int(row[9])
                self.packet_size = int(row[10])
                self.sys_status_file = str(row[11])
                self.data_dir = str(row[12])
                self.data_dir_path = str(row[13])
                self.db_file = str(row[14])
                self.log_dir = str(row[15])
                self.br_log_name = str(row[16])
                self.br_log_file = str(row[17])
                self.bs_log_name = str(row[18])
                self.bs_log_file = str(row[19])
                self.wt_changed = int(row[20])
                self.pipo_scor_wt = float(row[21])
                self.pipo_qual_wt = float(row[22])
                self.pipo_size_wt = float(row[23])
                self.pipo_trig_wt = float(row[24])
                self.pipo_time_wt = float(row[25])
                self.purge_rating = float(row[26])
                self.max_msg_size = int(row[27])
                dbc.close()
                self.nodb = False
            else:
                # Database is not there yet; instantiate manually.
                #print "DB not there??"
                self.nodb = True
        except: # Exception as e:
            # Some kind of DB failure; instantiate manually.\
            #print "Some kind of DB failure: " + str(e)
            self.nodb = True

        if self.nodb:
            self.cfgfile = bot_cfg_file
            self.success, self.bot_cfg_json = bothelp.readFloatFile(self.cfgfile, False, True)

            # If reading the specified configuration file fails, construct
            # a FACTORY DEFAULT CONFIG FILE that provides enough information
            # to continue running and, eventually, notify the Cloud of the
            # problem.  We'll assume we are running on the Float in a live
            # environment, but capture a modicum of logging infomation for
            # possible analysis later. Turn off debug mode (which implies
            # console output).
            if not self.success:
                # Factory Defaults are set here. This now relies on the
                # code-level "Machine" variable.
                if Machine == Machines.FLOAT:
                    FACTORY_MACHINE = int(Machines.FLOAT)
                    FACTORY_DBG  = -1
                    FACTORY_LOG  = -1
                    FACTORY_TIME = 1
                    FACTORY_LOCK = 0
                    FACTORY_TYPE = "Iridium"
                    FACTORY_HOST = "52.38.4.219"
                    FACTORY_PORT = 8000
                    FACTORY_PROTOCOL = 1
                    FACTORY_PACKET_SIZE = 370
                    FACTORY_STATUS_FILE = "sys_status_file"
                    FACTORY_DATA_DIR = "data"
                    FACTORY_LOG_DIR = "log"
                    FACTORY_BR_LOG_NAME = "brlog.txt"
                    FACTORY_BS_LOG_NAME = "bslog.txt"
                    FACTORY_WT_CHANGED = 0
                    FACTORY_PIPO_SCOR_WT = 0.5
                    FACTORY_PIPO_QUAL_WT = 0.5
                    FACTORY_PIPO_SIZE_WT = 0.5
                    FACTORY_PIPO_TRIG_WT = 0.5
                    FACTORY_PIPO_TIME_WT = 1.0
                    FACTORY_PURGE_RATING = 0.05
                    FACTORY_MAX_MSG_SIZE = 1000000
                elif Machine == Machines.TEST:
                    FACTORY_MACHINE = int(Machines.TEST)
                    FACTORY_DBG  = 3
                    FACTORY_LOG  = 3
                    FACTORY_TIME = 1
                    FACTORY_LOCK = 0
                    FACTORY_TYPE = "Ethernet"
                    FACTORY_HOST = "18.223.151.16"
                    FACTORY_PORT = 3002
                    FACTORY_PROTOCOL = 1
                    FACTORY_PACKET_SIZE = 1024
                    FACTORY_STATUS_FILE = "sys_status_file"
                    FACTORY_DATA_DIR = "data"
                    FACTORY_LOG_DIR = "log"
                    FACTORY_BR_LOG_NAME = "brlog.txt"
                    FACTORY_BS_LOG_NAME = "bslog.txt"
                    FACTORY_WT_CHANGED = 0
                    FACTORY_PIPO_SCOR_WT = 0.5
                    FACTORY_PIPO_QUAL_WT = 0.5
                    FACTORY_PIPO_SIZE_WT = 0.5
                    FACTORY_PIPO_TRIG_WT = 0.5
                    FACTORY_PIPO_TIME_WT = 1.0
                    FACTORY_PURGE_RATING = "0.05"
                    FACTORY_MAX_MSG_SIZE = 2000000
                else:
                    FACTORY_MACHINE = int(Machines.ALPHA)
                    FACTORY_DBG  = 5
                    FACTORY_LOG  = 5
                    FACTORY_TIME = 1
                    FACTORY_LOCK = 0
                    FACTORY_TYPE = "Ethernet"
                    FACTORY_HOST = "10.0.0.116"
                    FACTORY_PORT = 7770
                    FACTORY_PROTOCOL = 1
                    FACTORY_PACKET_SIZE = 1024
                    FACTORY_STATUS_FILE = "sys_status_file"
                    FACTORY_DATA_DIR = "data"
                    FACTORY_LOG_DIR = "log"
                    FACTORY_BR_LOG_NAME = "brlog.txt"
                    FACTORY_BS_LOG_NAME = "bslog.txt"
                    FACTORY_WT_CHANGED = 0
                    FACTORY_PIPO_SCOR_WT = 0.5
                    FACTORY_PIPO_QUAL_WT = 0.5
                    FACTORY_PIPO_SIZE_WT = 0.5
                    FACTORY_PIPO_TRIG_WT = 0.5
                    FACTORY_PIPO_TIME_WT = 1.0
                    FACTORY_PURGE_RATING = 0.05
                    FACTORY_MAX_MSG_SIZE = 2000000

                self.bot_cfg_json = {
                    "machine": FACTORY_MACHINE,
                    "debugging": FACTORY_DBG,
                    "logging": FACTORY_LOG,
                    "timing": FACTORY_TIME,
                    "locking": FACTORY_LOCK,
                    "type": FACTORY_TYPE,
                    "host": FACTORY_HOST,
                    "port": FACTORY_PORT,
                    "protocol": FACTORY_PROTOCOL,
                    "packet_size": FACTORY_PACKET_SIZE,
                    "sys_status_file": FACTORY_STATUS_FILE,
                    "data_dir": FACTORY_DATA_DIR,
                    "log_dir": FACTORY_LOG_DIR,
                    "br_log_name": FACTORY_BR_LOG_NAME,             
                    "bs_log_name": FACTORY_BS_LOG_NAME,
                    "wt_changed": FACTORY_WT_CHANGED,             
                    "pipo_scor_wt": FACTORY_PIPO_SCOR_WT,
                    "pipo_qual_wt": FACTORY_PIPO_QUAL_WT,
                    "pipo_size_wt": FACTORY_PIPO_SIZE_WT,
                    "pipo_trig_wt": FACTORY_PIPO_TRIG_WT,
                    "pipo_time_wt": FACTORY_PIPO_TIME_WT,
                    "purge_rating": FACTORY_PURGE_RATING,
                    "max_msg_size": FACTORY_MAX_MSG_SIZE
                }
                self.factory = True
            else:
                self.factory = False

            # Get the Bot Configuration information into useful
            # global variables.
            self.machine = int(self.bot_cfg_json["machine"])
            self.debugging = int(self.bot_cfg_json["debugging"])
            self.logging = int(self.bot_cfg_json["logging"])
            self.tracking = bool(self.debugging) or bool(self.logging)
            self.timing = int(self.bot_cfg_json["timing"])
            self.locking = int(self.bot_cfg_json["locking"])
            self.type = str(self.bot_cfg_json["type"])
            self.host = str(self.bot_cfg_json["host"])
            self.port = int(self.bot_cfg_json["port"])
            self.protocol = int(self.bot_cfg_json["protocol"])
            self.packet_size = int(self.bot_cfg_json["packet_size"])
            self.sys_status_file = str(self.bot_cfg_json["sys_status_file"])
            self.data_dir = str(self.bot_cfg_json["data_dir"])
            self.data_dir_path = nepi_home + "/" + self.data_dir
            self.db_file = str(bot_db_file)
            self.log_dir = str(self.bot_cfg_json["log_dir"])
            self.br_log_name = str(self.bot_cfg_json["br_log_name"])
            self.br_log_file = nepi_home + "/" + self.log_dir + "/" + self.br_log_name
            self.bs_log_name = str(self.bot_cfg_json["bs_log_name"])
            self.bs_log_file = nepi_home + "/" + self.log_dir + "/" + self.bs_log_name
            self.wt_changed = int(self.bot_cfg_json["wt_changed"])
            self.pipo_scor_wt = float(self.bot_cfg_json["pipo_scor_wt"])
            self.pipo_qual_wt = float(self.bot_cfg_json["pipo_qual_wt"])
            self.pipo_size_wt = float(self.bot_cfg_json["pipo_size_wt"])
            self.pipo_trig_wt = float(self.bot_cfg_json["pipo_trig_wt"])
            self.pipo_time_wt = float(self.bot_cfg_json["pipo_time_wt"])
            self.purge_rating = float(self.bot_cfg_json["purge_rating"])
            self.max_msg_size = int(self.bot_cfg_json["max_msg_size"])

