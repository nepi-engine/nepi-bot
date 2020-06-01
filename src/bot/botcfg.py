########################################################################
##
# Module: botcfg.py
# --------------------
##
# (c) Copyright 2019 by Numurus LLC
##
# This document, and all information therein, is the property of
# Numurus LLC.  It is confidential and must not be made public or
# copied in any form.  It is loaned subject to return upon demand
# and is not to be used directly or indirectly in any way detrimental
# to our interests.
##
########################################################################

import os
import sys
import json
import platform
from botdefs import Machine, Machines
from botdefs import nepi_home, bot_cfg_file
from bothelp import readFloatFile

v_botcfg = "bot71-20190624"

########################################################################
# Class: BotCfg (Retrieve the NEPI-Bot Configuration File).
########################################################################


class BotCfg(object):

    def __init__(self):
        self.factory = False
        self.bot_cfg_json = None
        self.enum = None
        self.emsg = None
        self.lb_ip = None
        self.lb_iridium = None
        self.lb_rs232 = None
        self.hp_ip = None

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
            success, self.bot_cfg_json = readFloatFile(
                None, None, 0, self.cfgfile, False, True)
        except Exception as e:
            self.enum = "CFG001"
            self.emsg = "ERROR: [" + str(e) + "]"
            success = [False, self.enum, self.emsg]

        # If reading the specified configuration file fails, construct
        # a FACTORY DEFAULT CONFIG FILE that provides enough information
        # to continue running and, eventually, notify the Cloud of the
        # problem.  We'll assume we are running on the Float in a live
        # environment, but capture a modicum of logging infomation for
        # possible analysis later. Turn off debug mode (which implies
        # console output).
        if not success[0]:
            if self.system == "Windows" or self.system == "Linux":
                self.devclass = "float1"
                self.logging = 3
                self.debugging = self.logging
                self.timing = True
                self.locking = False
                self.comms = True
                self.fs_pct_used_warning = 75
                self.sys_status_file = "sys_status_file"
                self.db_dir = "db"
                self.db_name = "float.db"
                self.db_deletes = 1
                self.data_dir = "data"
                self.data_zlib = True
                self.data_msgpack = True
                self.log_dir = "log"
                self.log_clear = True
                self.bs_log_name = "bslog.txt"
                self.bu_log_name = "bulog.txt"
                self.conn_log = True
                self.conn_log_name = "connlog.txt"
                self.packet_log = True
                self.packet_log_name = "packetlog.txt"
                self.access_log = True
                self.access_log_name = "accesslog.txt"
                self.wt_changed = 0
                self.pipo_scor_wt = 0.5
                self.pipo_qual_wt = 0.5
                self.pipo_size_wt = 0.5
                self.pipo_trig_wt = 0.5
                self.pipo_time_wt = 1.0
                self.purge_rating = 0.05
                self.lb_ip.enabled = 1
                self.lb_ip.type = "ethernet"
                self.lb_ip.host = "127.0.0.1"  # "10.0.0.116"
                self.lb_ip.port = 50000  # 7770
                self.lb_ip.baud = 57600
                self.lb_ip.tout = 1
                self.lb_ip.isp_open_attm = 10
                self.lb_ip.isp_open_tout = 1
                self.lb_ip.protocol = 2
                self.lb_ip.packet_size = 1024
                self.lb_ip.max_msg_size = 1500
                self.lb_ip.encrypted = False
            else:
                self.devclass = "float1"
                self.debugging = -1
                self.logging = 11
                self.timing = True
                self.locking = False
                self.comms = True
                self.fs_pct_used_warning = 70
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
                self.bs_log_name = "bslog.txt"
                self.bu_log_name = "bulog.txt"
                self.conn_log = True
                self.conn_log_name = "connlog.txt"
                self.packet_log = True
                self.packet_log_name = "packetlog.txt"
                self.access_log = True
                self.access_log_name = "accesslog.txt"
                self.wt_changed = 0
                self.pipo_scor_wt = 0.5
                self.pipo_qual_wt = 0.5
                self.pipo_size_wt = 0.5
                self.pipo_trig_wt = 0.5
                self.pipo_time_wt = 1.0
                self.purge_rating = 0.05
                self.lb_iridium.enabled = 1
                self.lb_iridium.type = "iridium"
                self.lb_iridium.host = "N/A"
                self.lb_iridium.port = "/dev/ttyUL0"
                self.lb_iridium.tout = 3
                self.lb_iridium.isp_open_attm = 10
                self.lb_iridium.isp_open_tout = 1
                self.lb_iridium.protocol = 1
                self.lb_iridium.max_msg_size = 340
                self.lb_iridium.packet_size = 340
                self.lb_iridium.encrypted = False
                #self.max_msg_size = 1020

            self.tracking = bool(self.debugging) or bool(self.logging)
            self.data_dir_path = nepi_home + "/" + self.data_dir
            self.db_file = nepi_home + "/" + self.db_dir + "/" + self.db_name
            self.bs_log_file = nepi_home + "/" + self.log_dir + "/" + self.bs_log_name
            self.bu_log_file = nepi_home + "/" + self.log_dir + "/" + self.bu_log_name
            self.conn_log_file = nepi_home + "/" + self.log_dir + "/" + self.conn_log_name
            self.packet_log_file = nepi_home + "/" + \
                self.log_dir + "/" + self.packet_log_name
            self.access_log_file = nepi_home + "/" + \
                self.log_dir + "/" + self.access_log_name
            self.factory = True
        else:
            # Get the Bot Configuration information into useful
            # global variables.
            self.devclass = str(self.bot_cfg_json["devclass"])

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
            self.comms = int(self.fs_pct_used_warning["fs_pct_used_warning"])
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
            self.db_deletes = bool(self.bot_cfg_json["db_deletes"])
            self.db_file = nepi_home + "/" + self.db_dir + "/" + self.db_name
            self.log_dir = str(self.bot_cfg_json["log_dir"])
            self.log_clear = bool(self.bot_cfg_json["log_clear"])
            self.bs_log_name = str(self.bot_cfg_json["bs_log_name"])
            self.bs_log_file = nepi_home + "/" + self.log_dir + "/" + self.bs_log_name
            self.bu_log_name = str(self.bot_cfg_json["bu_log_name"])
            self.bu_log_file = nepi_home + "/" + self.log_dir + "/" + self.bu_log_name
            self.conn_log = bool(self.bot_cfg_json["conn_log"])
            self.conn_log_name = str(self.bot_cfg_json["conn_log_name"])
            self.packet_log = bool(self.bot_cfg_json["packet_log"])
            self.wt_changed = bool(self.bot_cfg_json["wt_changed"])
            self.pipo_scor_wt = float(self.bot_cfg_json["pipo_scor_wt"])
            self.pipo_qual_wt = float(self.bot_cfg_json["pipo_qual_wt"])
            self.pipo_size_wt = float(self.bot_cfg_json["pipo_size_wt"])
            self.pipo_trig_wt = float(self.bot_cfg_json["pipo_trig_wt"])
            self.pipo_time_wt = float(self.bot_cfg_json["pipo_time_wt"])
            self.purge_rating = float(self.bot_cfg_json["purge_rating"])
            self.lb_iridium.enabled = True
            self.lb_iridium.type = "iridium"
            self.lb_iridium.host = "N/A"
            self.lb_iridium.port = "/dev/ttyUL0"
            self.lb_iridium.tout = 3
            self.lb_iridium.isp_open_attm = 10
            self.lb_iridium.isp_open_tout = 1
            self.lb_iridium.protocol = 1
            self.lb_iridium.max_msg_size = 340
            self.lb_iridium.packet_size = 340
            self.lb_iridium.encrypted = False
            self.max_msg_size = int(self.bot_cfg_json["max_msg_size"])
