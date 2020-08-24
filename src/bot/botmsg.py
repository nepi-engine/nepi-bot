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

            if isinstance(_value, str):
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

        # self.buf.append(self.buf1)
        # self.buf_str.append(self.buf_str1)
        msgs_outgoing.append(self.buf1)

        if self.cfg.tracking:
            self.log.track(_lev + 1, "Status Record PACKED.", True)
            # self.log.track(_lev + 2, "Msg Str: " + self.buf_str, True)
            # self.log.track(
            #     _lev + 2, "Msg Enc: " + str(self.buf, encoding="utf-8"), True
            # )
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
            #data_message_nepi_bot.nepi_bot_status_flags = 2

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

        # self.buf.append(self.buf1)
        # self.buf_str.append(self.buf_str1)

        msgs_outgoing.append(self.buf1)

        if self.cfg.tracking:
            self.log.track(_lev + 1, "DATA Record ENCODED.", True)
            # self.log.track(_lev + 2, "Msg Str: " + self.buf_str, True)
            # self.log.track(
            #     _lev + 2, "Msg Enc: " + str(self.buf, encoding="utf-8"), True
            # )
            self.log.track(_lev + 2, "Msg Siz: " + str(self.len), True)

        return [True, None, None]

    # -------------------------------------------------------------------
    # The 'encode_gen_msg()' Class Library Method. Protobuf implementation
    # -------------------------------------------------------------------
    def encode_gen_msg(self, _lev, _rec, _identifier, _value, _orig, _dev_id_bytes, db):

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
            # if isinstance(_identifier, int):
            #     gen_message_payload.int64_identifier = int(_identifier)
            # else:
            #     gen_message_payload.str_identifier = str(_identifier)
            #
            # if isinstance(_value, double):
            #     gen_message_payload.double_val = _value
            # elif isinstance(_value, float):
            #     gen_message_payload.float_val = _value
            # elif isinstance(_value, np.int64):
            #     gen_message_payload.int_64_val = _value
            # elif isinstance(_value, np.uint64):
            #     gen_message_payload.uint64_val = _value
            # elif isinstance(_value, bool):
            #     gen_message_payload.bool_val = _value
            # elif isinstance(_value, str):
            #     gen_message_payload.string_val = _value
            # else:
            #     gen_message_payload.raw_val = bytes(_value)
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
        # self.buf.append(self.buf1)
        # self.buf_str.append(self.buf_str1)

        msgs_outgoing.append(self.buf1)

        if self.cfg.tracking:
            self.log.track(_lev + 1, "GENERAL Record ENCODED.", True)
            self.log.track(_lev + 2, "Msg Str: " + self.buf_str1, True)
            self.log.track(
                _lev + 2, "Msg Enc: " + str(self.buf1, encoding="utf-8"), True
            )
            self.log.track(_lev + 2, "Msg Siz: " + str(self.len), True)

        return [True, None, None]

    # -------------------------------------------------------------------
    # The 'decode_server_msg()' Class Library Method. Protobuf implementation
    # -------------------------------------------------------------------
    def decode_server_msg(self, _lev, _nepi_msg, _dev_id_bytes, _msgs_incoming):

        try:
            nepi_msg = nepi_messaging_all_pb2.NEPIMsg()
            nepi_msg.ParseFromString(_nepi_msg)
        except Exception as e:
            enum = "MSG110"
            emsg = "decode_server_msg(): Problem decoding message from Server"
            if self.cfg.tracking:
                self.log.track(_lev, str(enum) + ": " + str(emsg), True)
                self.log.track(_lev, "Message Contents:")
                self.log.track(_lev, str(_nepi_msg))

            return [False, str(enum), str(emsg)]

        buf_json = json_format.MessageToJson(nepi_msg)
        if self.cfg.tracking:
            self.log.track(_lev, "Message received from Server:")
            self.log.track(_lev, str(buf_json))

        # There should only be config/general messages from Server to Bot/Device

        svr_comm_index = nepi_msg.comm_index
        svr_nuid = nepi_msg.nuid

        # check nuid in message to ensure we are the correct recipient
        if svr_nuid != _dev_id_bytes:
            enum = "MSG111"
            emsg = f"decode_server_msg(): Message incorrectly sent to this NEPI-Bot."
            if self.cfg.tracking:
                self.log.track(_lev, emsg)
                self.log.track(
                    _lev, "  NEPI-Bot NUID: " + _dev_id_bytes.decode("utf-8")
                )
                self.log.track(
                    _lev, "  Message NUID:  " + str(svr_nuid.decode("utf-8"))
                )

            return [False, str(enum), str(emsg)]

        msg_type = nepi_msg.WhichOneOf("msg")

        # general message type

        if msg_type == "gen_msg":

            msg_gen_msg = nepi_msg.gen_msg
            msg_routing = msg_gen_msg.routing.subsystem
            msg_payload = msg_gen_msg.msg_payload

            # retrieve identifier

            idtype = msg_payload.WhichOneof("identifier")
            if idtype == "str_identifier":
                midentifier = msg_payload.str_identifier
            else:
                midentifier = msg_payload.num_identifier

            # retrieve value

            valtype = msg_payload.WhichOneof("value")
            if valtype == "double_val":
                mvalue = msg_payload.double_val
            elif valtype == "float_val":
                mvalue = msg_payload.float_val
            elif valtype == "int64_val":
                mvalue = msg_payload.int64_val
            elif valtype == "uint64":
                mvalue = msg_payload.uint64
            elif valtype == "bool_val":
                mvalue = msg_payload.bool_val
            elif valtype == "string_val":
                mvalue = msg_payload.string_val
            else:
                mvalue = msg_payload.raw_val
            # if msg_payload.HasField("double_val"):
            #     mvalue = msg_payload.double_val
            # elif msg_payload.HasField("float_val"):
            #     mvalue = msg_payload.float_val
            # elif msg_payload.HasField("int64_val"):
            #     mvalue = msg_payload.int64_val
            # elif msg_payload.HasField("uint64"):
            #     mvalue = msg_payload.uint64
            # elif msg_payload.HasField("bool_val"):
            #     mvalue = msg_payload.string_val
            # else:
            #     mvalue = msg_payload.raw_val

            # add to incoming message list
            _msgs_incoming.append([msg_routing, midentifier, mvalue])

        # config message type

        elif msg_type == "cfg_msg":

            msg_cfg_msg = nepi_msg.cfg_msg
            msg_routing = msg_cfg_msg.routing.subsystem
            msg_cfg_vals = msg_cfg_msg.cfg_val
            idtype = msg_cfg_vals.WhichOneof("identifier")
            if idtype == "str_identifier":
                midentifier = msg_cfg_vals.str_identifier
            else:
                midentifier = msg_cfg_vals.num_identifier

            # retrieve value
            valtype = msg_cfg_vals.WhichOneof("value")
            if valtype == "double_val":
                mvalue = msg_cfg_vals.double_val
            elif valtype == "float_val":
                mvalue = msg_cfg_vals.float_val
            elif valtype == "int64_val":
                mvalue = msg_cfg_vals.int64_val
            elif valtype == "uint64":
                mvalue = msg_cfg_vals.uint64
            elif valtype == "bool_val":
                mvalue = msg_cfg_vals.bool_val
            elif valtype == "string_val":
                mvalue = msg_cfg_vals.string_val
            else:
                mvalue = msg_cfg_vals.raw_val
            # valtype = msg_cfg_vals.WhichOneof('value')
            # if valtype == 'double_val':
            #     mvalue = msg_cfg_vals.double_val
            # elif valtype == 'float_val':
            #     mvalue = msg_cfg_vals.float_val
            # elif valtype == 'int64_val':
            #     mvalue = msg_cfg_vals.int64_val
            # elif valtype == 'uint64':
            #     mvalue = msg_cfg_vals.uint64
            # elif valtype == 'bool_val':
            #     mvalue = msg_cfg_vals.bool_val
            # elif valtype == 'string_val':
            #     mvalue = msg_cfg_vals.string_val
            # else:
            #     mvalue = msg_cfg_vals.raw_val
            _msgs_incoming.append([msg_routing, midentifier, mvalue])
        else:
            pass

        return [True, None, None], _msgs_incoming
