########################################################################
##
# Module: botmsg.py
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
import datetime
import json
from enum import Enum

import numpy.core as np
from google.protobuf import json_format

import nepi_messaging_all_pb2
from botdefs import msgs_outgoing, msgs_incoming

# from numpy.core import double, int32, uint32, int64, uint64 as np

v_botmsg = "bot71-20200601"

########################################################################
# The Bot Message Class Library For Both Server and Device
########################################################################


class BotMsg(object):

    # ------------------------------------------------------------------
    # Initialize the BotMsg Class Library Object.
    # ------------------------------------------------------------------

    def __init__(self, _cfg, _log, db, _lev):
        self.cfg = _cfg
        self.log = _log
        self.lev = _lev
        self.buf = []
        self.buf1 = ""
        self.buf_str = []
        self.buf_str1 = ""
        self.len = 0
        self.type = None

    class MsgType(Enum):
        STATUS = 1
        METADATA = 2
        CONFIG = 3
        GENERAL = 4

    class ObjEncoder(json.JSONEncoder):
        def default(self, obj):
            # print('default(', repr(obj), ')')
            return obj.__dict__

    # convert python value to smallest best match SystemValue NEPImsg protobuf type
    # for the wire.
    # return False if successful. Otherwise log error and return True.
    def convert_to_system_value(self, _sysval, _identifier, _value):
        try:
            if isinstance(_identifier, int) and np.can_cast(
                _identifier, "uint32", "safe"
            ):
                _sysval.num_identifier = np.uint32(_identifier)
            else:
                _sysval.str_identifier = str(_identifier)

            if isinstance(_value, list):
                _sysval.raw_val = bytes(_value)
            elif isinstance(_value, str):
                _sysval.string_value = str(_value)
            elif isinstance(_value, bool):
                _sysval.bool_val = _value
            elif isinstance(_value, int) and np.can_cast(_value, "uint64", "safe"):
                _sysval.uint64_val = np.uint64(_value)
            elif isinstance(_value, int) and np.can_cast(_value, "int64", "safe"):
                _sysval.uint64_val = np.int64(_value)
            elif isinstance(_value, float) and np.can_cast(_value, "float32", "safe"):
                _sysval.float_val = np.float32(_value)
            elif isinstance(_value, float) and np.can_cast(_value, "float64", "safe"):
                _sysval.double_val = np.float64(_value)
            else:
                _sysval.raw_val = bytes(_value)
        except Exception as e:
            enum = "MSG102"
            emsg = f"convert_to_system_value(): Problem converting SystemValue for protobuf [{e}]."
            if self.cfg.tracking:
                self.log.track(0, str(enum) + ": " + str(emsg), True)
            return True
        return False

    # -------------------------------------------------------------------
    # The 'encode_status_msg()' Class Library Method. Protobuf implementation
    # -------------------------------------------------------------------
    def encode_status_msg(self, _lev, _rec, dev_id_bytes, db):

        if self.cfg.tracking:
            self.log.track(_lev, "Entering 'encode_status_msg()' Class Method.", True)
            self.log.track(_lev + 1, "_lev: " + str(_lev), True)
            self.log.track(_lev + 13, "_rec: " + str(_rec), True)

        try:
            ## Set NEPI Message
            nepi_msg = nepi_messaging_all_pb2.NEPIMsg()
            nepi_msg.comm_index = db.get_botcomm_index()
            nepi_msg.nuid = dev_id_bytes

            # Set system status
            system_status = nepi_msg.sys_status

            # Set Sys status components added by NEPI-BOT
            system_status_nepi_bot = system_status.nepi_bot_status
            system_status_nepi_bot.sys_status_id = 1
            system_status_nepi_bot.nepi_bot_status_flags = 2

            # Set Sys status components provided by device
            system_status_device = system_status.device_status
            dt = datetime.datetime.fromtimestamp(_rec[5], None)
            system_status_device.timestamp.FromDatetime(dt)
            system_status_device.navsat_fix_time_offset = np.int32(_rec[6])
            system_status_device.defined_latitude = float(_rec[7])
            system_status_device.defined_longitude = float(_rec[8])
            system_status_device.defined_heading = np.uint32(_rec[9])
            system_status_device.heading_true_north = int(_rec[10])
            system_status_device.roll = np.int32(_rec[11])
            system_status_device.pitch = np.int32(_rec[12])
            system_status_device.temperature = np.int32(_rec[13])
            system_status_device.power_state = np.uint32(_rec[14])
            system_status_device.device_status = bytes(_rec[15])
        except Exception as e:
            enum = "MSG102"
            emsg = (
                f"pack_status_msg(): Problem building status record for Server [{e}]."
            )
            if self.cfg.tracking:
                self.log.track(_lev, str(enum) + ": " + str(emsg), True)
            return [False, str(enum), str(emsg)]

        self.buf1 = nepi_msg.SerializeToString()
        self.buf_str1 = json_format.MessageToJson(
            nepi_msg,
            including_default_value_fields=False,
            preserving_proto_field_name=False,
            indent=0,
        )
        self.len = len(self.buf)

        msgs_outgoing.append(self.buf1)

        if self.cfg.tracking:
            self.log.track(_lev + 1, "Status Record PACKED.", True)
            self.log.track(_lev + 2, "Msg Siz: " + str(self.len), True)

        return [True, None, None]

    # -------------------------------------------------------------------
    # The 'encode_data_msg()' Class Library Method. Protobuf implementation
    # -------------------------------------------------------------------
    def encode_data_msg(self, _lev, _rec, dev_id_bytes, db):

        if self.cfg.tracking:
            self.log.track(_lev, "Entering 'encode_data_msg()' Class Method.", True)
            self.log.track(_lev + 1, "_lev: " + str(_lev), True)
            self.log.track(_lev + 13, "_rec: " + str(_rec), True)

        try:
            ## Set NEPI Message
            nepi_msg = nepi_messaging_all_pb2.NEPIMsg()
            nepi_msg.comm_index = db.get_botcomm_index()
            nepi_msg.nuid = dev_id_bytes

            # Set data message
            data_message = nepi_msg.data_msg

            # Set nepi-bot metadata
            data_message_nepi_bot = data_message.nepi_bot_metadata
            data_message_nepi_bot.sys_status_id = int(_rec[2])
            # data_message_nepi_bot.nepi_bot_status_flags = 2

            # Set nepi device data
            data_message_device_data = data_message.device_data
            data_message_device_data.type = str(_rec[5])
            data_message_device_data.instance = int(_rec[6])
            data_message_device_data.data_time_offset = int(_rec[7])
            data_message_device_data.latitude_offset = int(_rec[8])
            data_message_device_data.longitude_offset = int(_rec[9])
            data_message_device_data.heading_offset = int(_rec[10])
            data_message_device_data.roll_offset = int(_rec[11])
            data_message_device_data.pitch_offset = int(_rec[12])
            data_message_device_data.payload = bytes(_rec[15])
        except Exception as e:
            enum = "MSG102"
            emsg = f"encode_data_msg(): Problem building data record for Server [{e}]."
            if self.cfg.tracking:
                self.log.track(_lev, str(enum) + ": " + str(emsg), True)
            return [False, str(enum), str(emsg)]

        self.buf1 = nepi_msg.SerializeToString()
        self.buf_str1 = json_format.MessageToJson(
            nepi_msg,
            including_default_value_fields=False,
            preserving_proto_field_name=False,
            indent=0,
        )
        self.len = len(self.buf)

        msgs_outgoing.append(self.buf1)

        if self.cfg.tracking:
            self.log.track(_lev + 1, "DATA Record ENCODED.", True)
            self.log.track(_lev + 2, "Msg Siz: " + str(self.len), True)

        return [True, None, None]

    # -------------------------------------------------------------------
    # The 'encode_gen_msg()' Class Library Method. Protobuf implementation
    # -------------------------------------------------------------------
    def encode_gen_msg(self, _lev, _identifier, _value, _orig, _dev_id_bytes, db):

        # Set nepi msg
        try:
            nepi_msg = nepi_messaging_all_pb2.NEPIMsg()
            nepi_msg.nuid = _dev_id_bytes

            nepi_msg.comm_index = db.get_botcomm_index()

            # Set cfg message
            gen_message = nepi_msg.gen_msg

            # Set routing
            gen_message_routing = gen_message.routing
            gen_message_routing.subsystem = _orig

            # Set System Value
            gen_message_payload = gen_message.msg_payload
            retcode = self.convert_to_system_value(
                gen_message_payload, _identifier, _value
            )

        except Exception as e:
            enum = "MSG103"
            emsg = (
                f"encode_gen_msg(): Problem building general record for Server [{e}]."
            )
            if self.cfg.tracking:
                self.log.track(_lev, str(enum) + ": " + str(emsg), True)
            return [False, str(enum), str(emsg)]

        self.buf1 = nepi_msg.SerializeToString()
        self.buf_str1 = json_format.MessageToJson(
            nepi_msg,
            including_default_value_fields=False,
            preserving_proto_field_name=False,
            indent=0,
        )

        self.len = len(self.buf)

        msgs_outgoing.append(self.buf1)

        if self.cfg.tracking:
            self.log.track(_lev + 1, "GENERAL Record ENCODED.", True)
            self.log.track(_lev + 2, "Msg Str: " + self.buf_str1, True)
            self.log.track(_lev + 2, "Msg Siz: " + str(self.len), True)

        return [True, None, None]

    # -------------------------------------------------------------------
    # The 'decode_server_msg()' Class Library Method. Protobuf implementation
    # -------------------------------------------------------------------
    def decode_server_msg(self, _lev, _nepi_msg, _dev_id_bytes):

        try:
            nepi_msg = nepi_messaging_all_pb2.NEPIMsg()
            nepi_msg.ParseFromString(_nepi_msg)
        except Exception as e:
            enum = "MSG110"
            emsg = "decode_server_msg(): Problem decoding message from Server"
            if self.cfg.tracking:
                self.log.track(_lev, str(enum) + ": " + str(emsg), True)
                self.log.track(_lev, "Message Contents:", True)
                self.log.track(_lev, str(_nepi_msg), True)

            return [False, str(enum), str(emsg)], 0, 0, [], "", ""

        buf_json = json_format.MessageToJson(nepi_msg)

        # There should only be config/general messages from Server to Bot/Device
        svr_comm_index = nepi_msg.comm_index
        svr_nuid = nepi_msg.nuid

        # check nuid in message to ensure we are the correct recipient
        # if svr_nuid != _dev_id_bytes:
        #     enum = "MSG111"
        #     emsg = f"decode_server_msg(): Message incorrectly sent to this NEPI-Bot."
        #     if self.cfg.tracking:
        #         self.log.track(_lev, emsg, True)
        #         self.log.track(
        #             _lev, "  NEPI-Bot NUID: " + _dev_id_bytes.decode("utf-8"), True
        #         )
        #         self.log.track(
        #             _lev, "  Message NUID:  ", True  #+ str(svr_nuid.decode("utf-8")), True
        #         )
        #     return [False, str(enum), str(emsg)], 0, svr_comm_index, "", [], buf_json, ""
        # else:
        #     enum = "MSG112"
        #     emsg = f"decode_server_msg(): Now processing Server message."
        #     if self.cfg.tracking:
        #         self.log.track(_lev, emsg, True)

        msg_type = nepi_msg.WhichOneOf("msg")

        # general message type

        if msg_type == "gen_msg":

            msg_gen_msg = nepi_msg.gen_msg
            msg_routing = msg_gen_msg.routing.subsystem
            msg_payload = msg_gen_msg.msg_payload

            # retrieve identifier/payload

            msg_stack = dict()
            ident = getattr(msg_payload, msg_payload.WhichOneof("identifier"))
            val = getattr(msg_payload, msg_payload.WhichOneof("value"))
            if isinstance(val, bytes):
                val = list(val)
            msg_stack[ident] = val

        # config message type

        elif msg_type == "cfg_msg":

            msg_cfg_msg = nepi_msg.cfg_msg
            msg_routing = msg_cfg_msg.routing.subsystem
            msg_cfg_vals = msg_cfg_msg.cfg_val

            msg_stack = dict()
            for i in msg_cfg_vals.cfg_val:
                ident = getattr(i, i.WhichOneof("identifier"))
                val = getattr(i, i.WhichOneof("value"))
                if isinstance(val, bytes):
                    val = list(val)
                msg_stack[ident] = val
        else:
            return [False, None, None], 0, svr_comm_index, msg_type, [], buf_json, ""

        # format json for device file
        dev_json_str = json.dumps(
            msg_stack, ensure_ascii=True, allow_nan=True, indent=4
        )

        return (
            [True, None, None],
            int(msg_routing),
            svr_comm_index,
            msg_type,
            msg_stack,
            buf_json,
            dev_json_str
        )
