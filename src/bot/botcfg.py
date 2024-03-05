#
# Copyright (c) 2024 Numurus, LLC <https://www.numurus.com>.
#
# This file is part of nepi-engine
# (see https://github.com/nepi-engine).
#
# License: 3-clause BSD, see https://opensource.org/licenses/BSD-3-Clause
#

import os
import sys
import json
import platform
from botdefs import Machine, Machines
from botdefs import nepi_home, bot_cfg_file
from bothelp import readFloatFile

v_botcfg = "bot71-20200601"

########################################################################
# Class: BotCfg (Retrieve the NEPI-Bot Configuration File).
#######################################################################


class obj(object):
    pass


class BotCfg(object):
    def __init__(self):
        self.factory = False
        self.bot_cfg_json = None
        self.enum = None
        self.emsg = None
        self.lb_ip = obj()  # None
        self.lb_iridium = obj()  # = None
        self.lb_rs232 = obj()  # None
        self.hb_ip = obj()  # None
        self.lb_conn_order = obj()  # None
        self.hb_conn_order = obj()  # None

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
                None, None, 0, self.cfgfile, False, True
            )
        except Exception as e:
            self.enum = "CFG001"
            self.emsg = "ERROR: [" + str(e) + "]"
            success = [False, self.enum, self.emsg]

        self.alias = str(self.bot_cfg_json.get("alias", "nepi-bot-simulation"))
        self.devclass = str(self.bot_cfg_json.get("devclass", "generic"))
        self.logging = int(self.bot_cfg_json.get("logging", 23))
        if int(self.logging) < -1:
            self.logging = -1
        if int(self.logging) > 23:
            self.logging = 23
        self.debugging = self.logging

        if (int(self.logging) > -1) or (int(self.debugging) > -1):
            self.tracking = True
        else:
            self.tracking = False

        self.timing = True
        self.locking = bool(self.bot_cfg_json.get("locking", 0))
        self.comms = bool(self.bot_cfg_json.get("comms", 1))
        self.fs_pct_used_warning = int(self.bot_cfg_json.get("fs_pct_used_warning", 75))
        self.sys_status_file = str(
            self.bot_cfg_json.get("sys_status_file", "sys_status.json")
        )
        self.lb_data_dir = str(self.bot_cfg_json.get("lb_data_dir", "lb/data"))
        self.lb_data_dir_path = "/".join((nepi_home, self.lb_data_dir))
        self.lb_cfg_dir = str(self.bot_cfg_json.get("lb_cfg_dir", "lb/cfg"))
        self.lb_cfg_dir_path = "/".join((nepi_home, self.lb_cfg_dir))
        self.lb_req_dir = str(self.bot_cfg_json.get("lb_req_dir", "lb/req"))
        self.lb_req_dir_path = "/".join((nepi_home, self.lb_req_dir))
        self.lb_resp_dir = str(self.bot_cfg_json.get("lb_resp_dir", "lb/resp"))
        self.lb_resp_dir_path = "/".join((nepi_home, self.lb_resp_dir))
        self.hb_data_dir = str(self.bot_cfg_json.get("hb_data_dir", "hb/data"))
        self.hb_data_dir_path = "/".join((nepi_home, self.hb_data_dir))
        self.hb_dir_outgoing = str(self.bot_cfg_json.get("hb_dir_outgoing", "hb/do"))
        self.hb_dir_outgoing_path = "/".join((nepi_home, self.hb_dir_outgoing))
        self.hb_dir_incoming = str(self.bot_cfg_json.get("hb_dir_incoming", "hb/dt"))
        self.hb_dir_incoming_path = "/".join((nepi_home, self.hb_dir_incoming))
        self.data_zlib = bool(self.bot_cfg_json.get("data_zlib", 1))
        self.data_msgpack = bool(self.bot_cfg_json.get("data_msgpack", 1))
        self.db_dir = str(self.bot_cfg_json.get("db_dir", "db"))
        self.db_name = str(self.bot_cfg_json.get("dbname", "nepibot.db"))
        self.db_deletes = bool(self.bot_cfg_json.get("db_deletes", 1))
        self.db_file = "/".join((nepi_home, self.db_dir, self.db_name))
        self.log_dir = str(self.bot_cfg_json.get("log_dir", "log"))
        self.log_clear = bool(self.bot_cfg_json.get("log_clear", 1))
        self.botmain_log_name = str(
            self.bot_cfg_json.get("botmain_log_name", "botmainlog.txt")
        )
        self.bs_log_file = "/".join((nepi_home, self.log_dir, self.botmain_log_name))

        self.conn_log = bool(self.bot_cfg_json.get("conn_log", 1))
        self.conn_log_name = str(
            self.bot_cfg_json.get("lb_conn_log_name", "lbconnlog.txt")
        )
        self.lb_conn_log_file = "/".join((nepi_home, self.log_dir, self.conn_log_name))
        self.packet_log = bool(self.bot_cfg_json.get("packet_log", 0))
        self.packet_log_name = str(
            self.bot_cfg_json.get("packet_log_name", "packetlog.txt")
        )
        self.packet_log_file = "/".join((nepi_home, self.log_dir, self.packet_log_name))
        self.lb_encrypted = bool(self.bot_cfg_json.get("lb_encrypted", 0))
        self.wt_changed = False

        # TODO: AGV - temp workaround
        self.pipo_scor_wt = float(self.bot_cfg_json.get("pipo_scor_wt", 0.5))
        self.pipo_qual_wt = float(self.bot_cfg_json.get("pipo_qual_wt", 0.5))
        self.pipo_size_wt = float(self.bot_cfg_json.get("pipo_size_wt", 0.5))
        self.pipo_time_wt = float(self.bot_cfg_json.get("pipo_time_wt", 1.0))
        self.pipo_trig_wt = float(self.bot_cfg_json.get("pipo_trig_wt", 0.5))
        self.purge_rating = float(self.bot_cfg_json.get("purge_rating", 0.05))

        self.lb_iridium.enabled = bool(
            self.bot_cfg_json.get("lb_iridium").get("enabled", 1)
        )
        self.lb_iridium.type = str(
            self.bot_cfg_json.get("lb_iridium").get("type", "iridium-sbd")
        )
        self.lb_iridium.port = str(
            self.bot_cfg_json.get("lb_iridium").get("port", "/dev/ttyUL0")
        )
        self.lb_iridium.baud = int(
            self.bot_cfg_json.get("lb_iridium").get("baud", 19200)
        )
        self.lb_iridium.tout = int(self.bot_cfg_json.get("lb_iridium").get("tout", 3))
        self.lb_iridium.open_attm = int(
            self.bot_cfg_json.get("lb_iridium").get("open_attm", 10)
        )
        self.lb_iridium.open_tout = int(
            self.bot_cfg_json.get("lb_iridium").get("open_tout", 1)
        )
        self.lb_iridium.protocol = int(
            self.bot_cfg_json.get("lb_iridium").get("protocol", 1)
        )
        self.lb_iridium.max_msg_size = int(
            self.bot_cfg_json.get("lb_iridium").get("max_msg_size", 340)
        )
        self.lb_iridium.packet_size = int(
            self.bot_cfg_json.get("lb_iridium").get("packet_size", 340)
        )
        self.lb_iridium.pipo_scor_wt = float(
            self.bot_cfg_json.get("lb_iridium").get("pipo_scor_wt", 0.5)
        )
        self.lb_iridium.pipo_qual_wt = float(
            self.bot_cfg_json.get("lb_iridium").get("pipo_qual_wt", 0.5)
        )
        self.lb_iridium.pipo_size_wt = float(
            self.bot_cfg_json.get("lb_iridium").get("pipo_size_wt", 0.5)
        )
        self.lb_iridium.pipo_time_wt = float(
            self.bot_cfg_json.get("lb_iridium").get("pipo_time_wt", 1.0)
        )
        self.lb_iridium.pipo_trig_wt = float(
            self.bot_cfg_json.get("lb_iridium").get("pipo_trig_wt", 0.5)
        )
        self.lb_iridium.purge_rating = float(
            self.bot_cfg_json.get("lb_iridium").get("purge_rating", 0.05)
        )

        self.lb_ip.enabled = bool(self.bot_cfg_json.get("lb_ip").get("enabled", 1))
        self.lb_ip.type = str(self.bot_cfg_json.get("lb_ip").get("type", "ethernet"))
        self.lb_ip.host = str(self.bot_cfg_json.get("lb_ip").get("host", "nepi.io"))
        self.lb_ip.port = str(self.bot_cfg_json.get("lb_ip").get("port", "50000"))
        self.lb_ip.tout = int(self.bot_cfg_json.get("lb_ip").get("tout", 3))
        self.lb_ip.open_attm = int(self.bot_cfg_json.get("lb_ip").get("open_attm", 2))
        self.lb_ip.open_tout = int(self.bot_cfg_json.get("lb_ip").get("open_tout", 1))
        self.lb_ip.protocol = int(self.bot_cfg_json.get("lb_ip").get("protocol", 2))
        self.lb_ip.max_msg_size = int(
            self.bot_cfg_json.get("lb_ip").get("max_msg_size", 1500)
        )
        self.lb_ip.packet_size = int(
            self.bot_cfg_json.get("lb_ip").get("packet_size", 1500)
        )
        self.lb_ip.pipo_scor_wt = float(
            self.bot_cfg_json.get("lb_ip").get("pipo_scor_wt", 0.5)
        )
        self.lb_ip.pipo_qual_wt = float(
            self.bot_cfg_json.get("lb_ip").get("pipo_qual_wt", 0.5)
        )
        self.lb_ip.pipo_size_wt = float(
            self.bot_cfg_json.get("lb_ip").get("pipo_size_wt", 0.5)
        )
        self.lb_ip.pipo_time_wt = float(
            self.bot_cfg_json.get("lb_ip").get("pipo_time_wt", 1.0)
        )
        self.lb_ip.pipo_trig_wt = float(
            self.bot_cfg_json.get("lb_ip").get("pipo_trig_wt", 0.5)
        )
        self.lb_ip.purge_rating = float(
            self.bot_cfg_json.get("lb_ip").get("purge_rating", 0.05)
        )

        self.lb_rs232.enabled = bool(
            self.bot_cfg_json.get("lb_rs232").get("enabled", 0)
        )
        self.lb_rs232.type = str(self.bot_cfg_json.get("lb_rs232").get("type", "rs232"))
        self.lb_rs232.port = str(
            self.bot_cfg_json.get("lb_rs232").get("port", "/dev/tty51")
        )
        self.lb_rs232.baud = int(self.bot_cfg_json.get("lb_rs232").get("baud", 19200))
        self.lb_rs232.tout = int(self.bot_cfg_json.get("lb_rs232").get("tout", 3))
        self.lb_rs232.open_attm = int(
            self.bot_cfg_json.get("lb_rs232").get("open_attm", 10)
        )
        self.lb_rs232.open_tout = int(
            self.bot_cfg_json.get("lb_rs232").get("open_tout", 1)
        )
        self.lb_rs232.protocol = int(
            self.bot_cfg_json.get("lb_rs232").get("protocol", 1)
        )
        self.lb_rs232.max_msg_size = int(
            self.bot_cfg_json.get("lb_rs232").get("max_msg_size", 340)
        )
        self.lb_rs232.packet_size = int(
            self.bot_cfg_json.get("lb_rs232").get("packet_size", 1500)
        )
        self.lb_rs232.pipo_scor_wt = float(
            self.bot_cfg_json.get("lb_rs232").get("pipo_scor_wt", 0.5)
        )
        self.lb_rs232.pipo_qual_wt = float(
            self.bot_cfg_json.get("lb_rs232").get("pipo_qual_wt", 0.5)
        )
        self.lb_rs232.pipo_size_wt = float(
            self.bot_cfg_json.get("lb_rs232").get("pipo_size_wt", 0.5)
        )
        self.lb_rs232.pipo_time_wt = float(
            self.bot_cfg_json.get("lb_rs232").get("pipo_time_wt", 1.0)
        )
        self.lb_rs232.pipo_trig_wt = float(
            self.bot_cfg_json.get("lb_rs232").get("pipo_trig_wt", 0.5)
        )
        self.lb_rs232.purge_rating = float(
            self.bot_cfg_json.get("lb_rs232").get("purge_rating", 0.05)
        )

        self.hb_ip.enabled = bool(self.bot_cfg_json.get("hb_ip").get("enabled", 1))
        self.hb_ip.type = str(self.bot_cfg_json.get("hb_ip").get("type", "ethernet"))
        self.hb_ip.host = str(self.bot_cfg_json.get("hb_ip").get("host", "nepi.io"))
        self.hb_ip.port = str(self.bot_cfg_json.get("hb_ip").get("port", "2222"))
        self.hb_ip.tout = int(self.bot_cfg_json.get("hb_ip").get("tout", 3))
        self.hb_ip.open_attm = int(self.bot_cfg_json.get("hb_ip").get("open_attm", 2))
        self.hb_ip.open_tout = int(self.bot_cfg_json.get("hb_ip").get("open_tout", 1))
        self.hb_ip.protocol = int(self.bot_cfg_json.get("hb_ip").get("protocol", 2))
        self.hb_ip.max_msg_size = int(
            self.bot_cfg_json.get("hb_ip").get("max_msg_size", 1500)
        )
        self.hb_ip.packet_size = int(
            self.bot_cfg_json.get("hb_ip").get("packet_size", 1500)
        )

        self.lb_conn_order = self.bot_cfg_json.get(
            "lb_conn_order", ["lb_ip", "lb_iridium", "lb_rs232"]
        )
        self.hb_conn_order = self.bot_cfg_json.get("hb_conn_order", ["hb_ip"])
