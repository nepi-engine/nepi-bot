########################################################################
##
# Module: botcomm.py
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

import struct
import socket
import sys
from pathlib import Path

import serial
import time
import os
import subprocess

from botdefs import (
    bot_devsshkeys_file,
    msgs_incoming,
    msgs_outgoing,
    bot_devnuid_file,
)

from bothelp import getDevId


v_botcomm = "bot71-20200601"

# processes that may need to be cleaned up before BOT exits
procs = []
# local port number for SSH client and UDP traffic
LOCALPORT = "8000"
LOCALHOST = "127.0.0.1"
retcode = None
send_delay_secs = 1.0  # amount of time to wait between sending messages to the server


# from botcfg import BotCfg
# from botlog import BotLog

########################################################################
# The Float Communications Class
########################################################################


class obj(object):
    pass


class BotComm(object):

    # -------------------------------------------------------------------
    # Initialize the BotComm Class w/ BotCfg, BotLog, and Type objects.
    # -------------------------------------------------------------------
    def __init__(self, _cfg, _log, _typ, _lev):
        self.cfg = _cfg
        self.log = _log
        self.typ = _typ
        self.con = None
        self.serialport = None
        self.dev_id_str, self.dev_id_bytes, self.remote_id_str = getDevId(
            self.cfg, self.log, 0, bot_devnuid_file
        )
        if self.cfg.tracking:
            self.log.track(_lev, "Created BotComm Class Object.", True)
            self.log.track(_lev + 13, "^cfg: " + str(self.cfg), True)
            self.log.track(_lev + 13, "^log: " + str(self.log), True)
            self.log.track(_lev + 13, "^typ: " + str(self.typ), True)
            self.log.track(_lev + 13, "_lev: " + str(_lev), True)

    # -------------------------------------------------------------------
    # getconn() Class Library (Establish the Communications Connection).
    # -------------------------------------------------------------------

    def getconn(self, _lev):

        global procs

        if self.cfg.tracking:
            self.log.track(_lev, "Establish Communcations Connection.", True)
            self.log.track(_lev + 13, "^typ: " + str(self.typ), True)
            self.log.track(_lev + 13, "_lev: " + str(_lev), True)

        # ---------------------------------------------------------------
        # Determine Comms Active Status; Bail Gracefully if Inactive.
        # ---------------------------------------------------------------
        if not self.isactive(_lev + 1):
            return [True, None, None]

        # ---------------------------------------------------------------
        # Open the 'Iridium' Serial Port.
        # ---------------------------------------------------------------
        if self.typ == "iridium":
            if self.cfg.tracking:
                self.log.track(_lev + 1, "Open the 'iridium' Serial Port.", True)

            try:
                self.serialport = serial.Serial()
                self.serialport.port = self.cfg.lb_iridium.port
                self.serialport.baudrate = self.cfg.lb_iridium.baud
                self.serialport.timeout = self.cfg.lb_iridium.tout

                if self.cfg.tracking:
                    self.log.track(
                        _lev + 14, "port: " + str(self.serialport.port), True
                    )
                    self.log.track(
                        _lev + 14, "baud: " + str(self.serialport.baudrate), True
                    )
                    self.log.track(
                        _lev + 14, "tout: " + str(self.serialport.timeout), True
                    )

                if not self.serialport.isOpen():
                    if self.cfg.tracking:
                        self.log.track(_lev + 1, "Open Serial Port", True)
                        self.log.track(
                            _lev + 14,
                            "^open_attm: " + str(self.cfg.lb_iridium.open_attm),
                            True,
                        )
                        self.log.track(
                            _lev + 14,
                            "^open_tout:  " + str(self.cfg.lb_iridium.open_tout),
                            True,
                        )

                    for incr in range(1, self.cfg.lb_iridium.open_attm + 1):
                        try:
                            if self.cfg.tracking:
                                self.log.track(_lev + 14, "Attempt #" + str(incr), True)
                            self.serialport.open()
                        except Exception as e:
                            pass

                        if self.serialport.isOpen():
                            break

                        time.sleep(self.cfg.open_tout)

                    if not self.serialport.isOpen():
                        raise Exception("Can't Get Port OPEN.")

                    if self.cfg.tracking:
                        self.log.track(_lev + 2, "Serial Port OPENED.", True)
                else:
                    if self.cfg.tracking:
                        self.log.track(_lev + 2, "Serial Port ALREADY Opened", True)

                command = [
                    b"AT+CGMI",
                    b"AT+CGMM",
                    b"AT+CGMR",
                    b"AT+CGSN",
                    b"AT+SBDMTA=0",
                    b"AT+SBDMTA?",
                ]

                isu_name = self.acquire_response(command[0])
                if self.cfg.tracking:
                    self.log.track(_lev + 14, "ISU Name: " + str(isu_name), True)

                isu_model_number = self.acquire_response(command[1])
                if self.cfg.tracking:
                    self.log.track(
                        _lev + 14, "ISU Model Number: " + str(isu_model_number), True
                    )

                isu_version = self.acquire_response(command[2])
                if self.cfg.tracking:
                    self.log.track(_lev + 14, "ISU Version: ", False)
                    if isu_version:
                        self.log.track(_lev + 14, "", True)
                        for line in isu_version.split("\n"):
                            if len(line) > 1:
                                self.log.track(_lev + 15, str(line), True)
                    else:
                        self.log.track(_lev + 14, "False", True)

                isu_imei = self.acquire_response(command[3])
                if self.cfg.tracking:
                    self.log.track(_lev + 14, "ISU IMEI: " + str(isu_imei), True)

                sbdring = self.acquire_response(command[5])
                if self.cfg.tracking:
                    self.log.track(
                        _lev + 14, "SBD Ring Indication: " + str(sbdring), True
                    )
                    self.log.track(
                        _lev + 14, "SBD Ring Indication: " + str(sbdring), True
                    )

                return [True, None, None]

            except Exception as e:
                enum = "BC101"
                emsg = "getconn(): [" + str(e) + "]"
                if self.cfg.tracking:
                    self.log.track(_lev + 2, str(enum) + ": " + str(emsg), True)
                return [False, str(enum), str(emsg)]

        # ---------------------------------------------------------------
        # Open the 'Ethernet' IP Connection.
        # ---------------------------------------------------------------
        elif self.typ == "ethernet":
            if self.cfg.tracking:
                self.log.track(
                    _lev + 1, "Instantiate the 'ethernet' IP Connection.", True
                )
                self.log.track(_lev + 1, "type: " + str(self.cfg.lb_ip.type), True)
                self.log.track(_lev + 1, "host: " + str(self.cfg.lb_ip.host), True)
                self.log.track(_lev + 1, "port: " + str(self.cfg.lb_ip.port), True)

            # if ssh or socat background processes are already running, terminate them and create a new tunnel
            if procs:
                self.log.track(
                    _lev + 1, "IP Active SSH and SOCAT processes detected.", True
                )
                for item in procs:
                    rc = item.poll()
                    if rc is None:
                        item.terminate()
                        item.wait()
                self.log.track(
                    _lev + 1, "IP Active SSH and SOCAT processes terminated.", True
                )
                procs = []
            # the ssh key file should be mode 600
            try:
                p = Path(bot_devsshkeys_file)
                p.chmod(0o600)
                self.log.track(
                    _lev + 1,
                    f"Successfully changed permissions on {bot_devsshkeys_file}.",
                    True,
                )
            except Exception as e:
                self.log.track(
                    _lev + 1,
                    f"Unable to change permissions on {bot_devsshkeys_file} [{e}].",
                    True,
                )

            # establish SSH connection to remote server and route local UDP traffic to SSH tunnel
            self.log.track(
                _lev + 1,
                "IP Attempting to establish SSH tunnel and redirect UDP port.",
                True,
            )

            # TODO change remote host and user and testkey
            args_sshcmd = [
                "ssh",
                "-T",
                "-p",
                self.cfg.lb_ip.port,
                "-o",
                "StrictHostKeyChecking=no",
                "-o",
                "GlobalKnownHostsFile=/dev/null",
                "-o",
                "UserKnownHostsFile=/dev/null",
                "-i",
                bot_devsshkeys_file,
                "-N",
                "-L",
                str(LOCALPORT + ":localhost:" + LOCALPORT),
                str(self.remote_id_str + "@" + self.cfg.lb_ip.host),
            ]
            args_socatcmd = [
                "socat",
                str("UDP4-LISTEN:" + LOCALPORT + ",fork,reuseaddr"),
                "TCP4:" + LOCALHOST + ":" + LOCALPORT,
            ]
            proc_ssh = subprocess.Popen(args_sshcmd)
            proc_socat = subprocess.Popen(args_socatcmd)
            procs.append(proc_ssh)
            procs.append(proc_socat)

            if self.con is None:
                for res in socket.getaddrinfo(
                    LOCALHOST, LOCALPORT, socket.AF_UNSPEC, socket.SOCK_DGRAM
                ):
                    af, socktype, proto, canonname, sa = res
                    try:
                        self.con = socket.socket(af, socktype, proto)
                        self.con.settimeout(3)  # (self.cfg.lb_ip.timeout)
                    except socket.error as msg:
                        enum = "BC111"
                        emsg = "IP getconn(): Host/Port Connect Problem [" + msg + "]."
                        if self.cfg.tracking:
                            self.log.track(_lev + 2, str(enum) + ": " + str(emsg), True)
                        self.con = None
                        continue
                    try:
                        self.con.connect(sa)
                    except socket.error as msg:
                        enum = "BC111A"
                        emsg = "IP getconn(): Host/Port Connect Problem."
                        if self.cfg.tracking:
                            self.log.track(_lev + 2, str(enum) + ": " + str(emsg), True)
                        self.con = None
                        continue
                    break

                if self.con is None:
                    enum = "BC111B"
                    emsg = "IP getconn(): Host/Port Connect Unsuccessful."
                    if self.cfg.tracking:
                        self.log.track(_lev + 2, str(enum) + ": " + str(emsg), True)
                    return [False, str(enum), str(emsg)]
                else:
                    enum = "BC111C"
                    emsg = "IP getconn(): Host/Port Connect Successful."
                    if self.cfg.tracking:
                        self.log.track(_lev + 2, str(enum) + ": " + str(emsg), True)
                    # TODO reexamine - need 2 sec delay before sending/receiving messages on open socket
                    time.sleep(2)
            else:
                if self.cfg.tracking:
                    self.log.track(_lev + 1, "IP Connection ALREADY Exists.", True)
            return [True, None, None]

        # ---------------------------------------------------------------
        # Open the 'Wi-Fi' IP Connection.
        # ---------------------------------------------------------------
        elif self.typ == "WiFi":
            if self.cfg.tracking:
                self.log.track(_lev + 1, "Instantiate 'Wi-Fi' IP Connection.", True)
                enum = "BC121"
                emsg = "getconn(): NOT Implemented Yet."
                if self.cfg.tracking:
                    self.log.errtrack(str(enum), str(emsg))
                return [False, str(enum), str(emsg)]

        else:
            enum = "BC122"
            emsg = "getconn(): Unknown Communication Type [" + str(self.typ) + "]"
            if self.cfg.tracking:
                self.log.errtrack(str(enum), str(emsg))
            return [False, str(enum), str(emsg)]

        if self.cfg.tracking:
            self.log.track(_lev, "Connection Established.", True)
        return [True, None, None]

    # -------------------------------------------------------------------
    # Receive a Message.
    # -------------------------------------------------------------------
    def receive(self, _lev, num):
        if self.cfg.tracking:
            self.log.track(_lev, "Receive Messages on Established Connection.", True)
            self.log.track(_lev + 13, "_lev: " + str(_lev), True)

        # ---------------------------------------------------------------
        # Determine Comms Active Status; Bail Gracefully if Inactive.
        # ---------------------------------------------------------------
        if not self.isactive(_lev + 1):
            return [True, None, None], []

        # ---------------------------------------------------------------
        # Receive on the 'Ethernet' IP Connection.
        # ---------------------------------------------------------------
        if self.typ == "iridium":
            try:
                response = self.acquire_response(b"AT+SBDWT")
                if response == True:
                    if self.cfg.tracking:
                        self.log.track(_lev, "SBD Modem Ready To Receive Message", True)

                    response = self.acquire_response("" + "\r")
                    mo_buffer = self.read_status(_lev, "mo buffer", int(response))

                    if mo_buffer == True:
                        success, cnc_msgs = self.initiate_sbd(_lev, "receive", num)
                        if self.cfg.tracking:
                            self.log.track(
                                _lev + 1,
                                "All messages received: " + str(cnc_msgs),
                                True,
                            )

                        if success:
                            return [True, None, None], cnc_msgs
                        else:
                            enum = "BC131"
                            emsg = "SBD Sesion Time Out"
                            if self.cfg.tracking:
                                self.log.errtrack(str(enum), str(emsg))

                            return [False, str(enum), str(emsg)], None

                    else:
                        enum = "BC132"
                        emsg = "Mailbox check couldn't be sent to the mobile originated buffer."
                        if self.cfg.tracking:
                            self.log.errtrack(str(enum), str(emsg))

                        return [False, str(enum), str(emsg)], None

                else:
                    enum = "BC133"
                    emsg = "Connection Error. SBD modem didn't get any response from the mailbox check."
                    if self.cfg.tracking:
                        self.log.errtrack(str(enum), str(emsg))

                    return [False, str(enum), str(emsg)], None

            except Exception as e:
                enum = "BC134"
                emsg = str(e)
                if self.cfg.tracking:
                    self.log.errtrack(str(enum), str(emsg))
                return [False, str(enum), str(emsg)], None

        # ---------------------------------------------------------------
        # Receive on the 'Ethernet' IP Connection.
        # ---------------------------------------------------------------
        if self.typ == "ethernet":
            # read from socket until there is no more data
            while True:
                try:
                    socket_recv_size = 4096
                    rec = self.con.recv(socket_recv_size)
                    msgs_incoming.append(rec)
                    if self.cfg.tracking:
                        self.log.track(0, f"{'*' * 80}", True)
                        self.log.track(0, f"SOCKET RECEIVE DATA FOR BUFFER:", True)
                        self.log.track(0, f"recvmaxsize:    {socket_recv_size}", True)
                        self.log.track(1, f"buf:            {str(rec)}", True)
                        self.log.track(1, f"len:            {len(rec)}", True)
                        self.log.track(1, f"memaddr:        {id(rec)}", True)
                        self.log.track(1, f"memsize:        {sys.getsizeof(rec)}", True)
                        self.log.track(
                            0,
                            f"INCOMING MESSAGE INFO AS LIST ITEM FOR LATER PROCESSING:",
                            True,
                        )
                        self.log.track(
                            1, f"buf:            {str(msgs_incoming[-1])}", True
                        )
                        self.log.track(
                            1, f"len:            {len(msgs_incoming[-1])}", True
                        )
                        self.log.track(
                            1, f"memaddr:        {id(msgs_incoming[-1])}", True
                        )
                        self.log.track(
                            1,
                            f"memsize:        {sys.getsizeof(msgs_incoming[-1])}",
                            True,
                        )
                        self.log.track(0, f"{'*' * 80}", True)
                except socket.timeout as e:  # no data available
                    enum = "BC140"
                    emsg = "No more data available on socket. " + str(e)
                    if self.cfg.tracking:
                        self.log.errtrack(str(enum), str(emsg))
                    break
                except Exception as e:  # something unexpected happened
                    enum = "BC140"
                    emsg = str(e)
                    if self.cfg.tracking:
                        self.log.errtrack(str(enum), str(emsg))
                    return [False, str(enum), str(emsg)], None

            return [True, None, None]

    # -------------------------------------------------------------------
    # Get a MT Message from the MT buffer
    # -------------------------------------------------------------------
    def get_mt_message(self, _lev):
        try:
            if self.cfg.tracking:
                self.log.track(_lev + 1, "Attempting Message Read(s).", True)

            # icnt = 0
            # results = []
            # tries = 0
            # while True:
            #     iline = self.serialport.readline()
            #     if self.cfg.tracking:
            #         self.log.track(_lev+2, "iline " + str(tries) + ":" + str(iline), True)
            #     if iline.strip() == "":
            #         if tries > 5:
            #             if self.cfg.tracking:
            #                 self.log.track(_lev+2, "DONE.", True)
            #             break
            #         else:
            #             tries = tries + 1
            #     else:
            #         results.append(iline)
            #         icnt = icnt + 1

            response = self.acquire_response(b"AT+SBDRB")
            if response is False:
                enum = "BC141"
                emsg = "Connection Error. MT Message couldn't be transferred."
                if self.cfg.tracking:
                    self.log.errtrack(str(enum), str(emsg))
            else:
                response = response[2:-2]
                if self.cfg.tracking:

                    self.log.track(
                        _lev + 1,
                        "Message: ",
                        True,  # + str(response.).encode("hex"), True
                    )
            return response

            # return [ True, None, None ], results

        except Exception as e:
            enum = "BC142"
            emsg = str(e)
            if self.cfg.tracking:
                self.log.errtrack(str(enum), str(emsg))
            if self.serialport.isOpen():
                self.serialport.close()
            return [False, str(enum), str(emsg)], None

    # -------------------------------------------------------------------
    # Send a Message.
    # -------------------------------------------------------------------
    def send(self, _lev, _msg, _num):
        global retcode
        if self.cfg.tracking:
            self.log.track(_lev, "Send Message on Established Connection.", True)
            self.log.track(_lev + 13, "_lev: " + str(_lev), True)
            # TODO Fix encoding
            # self.log.track(_lev+13, "_msg: " + str(_msg).encode("hex"), True)

        # ---------------------------------------------------------------
        # Determine Comms Active Status; Bail Gracefully if Inactive.
        # ---------------------------------------------------------------
        if not self.isactive(_lev + 1):
            return [True, None, None], []

        # ---------------------------------------------------------------
        # Send on the 'Iridium' Connection.
        # ---------------------------------------------------------------
        if self.typ == "iridium":
            try:
                msg_length = len(_msg)

                checksum = 0
                for ch in _msg:
                    checksum += ord(ch)

                new_msg = _msg + struct.pack(">H", checksum)

                response = self.acquire_response(b"AT+SBDWB=" + bytes(msg_length))
                if response == True:
                    if self.cfg.tracking:
                        self.log.track(_lev, "SBD Modem Ready To Receive Message", True)

                    response = self.acquire_response(new_msg)
                    mo_buffer = self.read_status(_lev, "mo buffer", int(response))

                    if mo_buffer == True:
                        success, cnc_msgs = self.initiate_sbd(_lev, "send", _num)
                        if self.cfg.tracking:
                            self.log.track(
                                _lev + 1,
                                "All messages received: " + str(cnc_msgs),
                                True,
                            )

                        if success:
                            return [True, None, None], cnc_msgs
                        else:
                            enum = "BC151"
                            emsg = "SBD Sesion Time Out"
                            if self.cfg.tracking:
                                self.log.errtrack(str(enum), str(emsg))

                            return [False, str(enum), str(emsg)], None

                    else:
                        enum = "BC152"
                        emsg = "MO Message couldn't be sent to the mobile originated buffer."
                        if self.cfg.tracking:
                            self.log.errtrack(str(enum), str(emsg))

                        return [False, str(enum), str(emsg)], None

                else:
                    enum = "BC153"
                    emsg = "Connection Error. SBD modem didn't get any response."
                    if self.cfg.tracking:
                        self.log.errtrack(str(enum), str(emsg))

                    return [False, str(enum), str(emsg)], None

                # Condition for +SBDWB
                # msg = self.acquire_response(b'AT+SBDWB={message size}')
                # elif msg == '3':
                #     print "SBD message size is not correct. It should be between 1-340 bytes"
                ###

            except Exception as e:
                enum = "BC154"
                emsg = str(e)
                if self.cfg.tracking:
                    self.log.errtrack(str(enum), str(emsg))
                return [False, str(enum), str(emsg)], None

        # ---------------------------------------------------------------
        # Send on the 'ethernet' Connection.
        # ---------------------------------------------------------------

        if self.typ == "ethernet":
            try:
                while len(msgs_outgoing):
                    msg = msgs_outgoing.pop(0)
                    retcode = self.con.sendall(msg)
                    self.log.track(
                        _lev + 1,
                        "Sent 1 message (" + str(len(msg)) + " bytes)",
                        True,
                    )
                    time.sleep(send_delay_secs)
                if retcode is None:
                    return [True, None, None], None
                else:
                    enum = "BC161"
                    emsg = "IP Data Not Sent."
                    return [False, str(enum), str(emsg)], None
            except Exception as e:
                enum = "BC162"
                emsg = "IP Data Not Sent."
                return [False, str(enum), str(emsg)], None

    # -------------------------------------------------------------------
    # close() Class Method (Close the Connection).
    # -------------------------------------------------------------------

    def close(self, _lev):
        global procs
        if self.cfg.tracking:
            self.log.track(_lev, "Close the Communications Connection.", True)
            self.log.track(_lev + 1, "_lev: " + str(_lev), True)
            self.log.track(_lev + 1, "^typ: " + str(self.typ), True)

        # ---------------------------------------------------------------
        # Determine Comms Active Status; Bail Gracefully if Inactive.
        # ---------------------------------------------------------------
        if not self.isactive(_lev + 1):
            return [True, None, None]

        # ---------------------------------------------------------------
        # Close the 'Iridium' Serial Port.
        # ---------------------------------------------------------------
        if self.typ == "iridium":
            if self.cfg.tracking:
                self.log.track(_lev + 1, "Close 'iridium' Serial Port.", True)

            try:
                if self.serialport.isOpen():
                    self.serialport.close()
                    if self.cfg.tracking:
                        self.log.track(_lev + 1, "Serial Port Closed.", True)
                else:
                    if self.cfg.tracking:
                        self.log.track(_lev + 1, "Serial Port Already Closed.", True)

                return [True, None, None]

            except Exception as e:
                enum = "BC161"
                emsg = "close(): FAILED [" + str(e) + "]"
                if self.cfg.tracking:
                    self.log.track(_lev + 1, str(enum) + ": " + str(emsg), True)

                return [False, str(enum), str(emsg)]

        # ---------------------------------------------------------------
        # Close the 'Ethernet' IP Connection.
        # ---------------------------------------------------------------
        elif self.typ == "ethernet":
            if self.cfg.tracking:
                self.log.track(_lev + 1, "Close 'ethernet' IP Connection.", True)

            if self.con is not None:
                try:
                    # stop reading/writing on socket before closing connection.
                    self.con.shutdown(socket.SHUT_RDWR)
                    self.con.close()
                    self.con = None

                    if self.cfg.tracking:
                        self.log.track(_lev + 1, "Comm Connection Closed.", True)

                except Exception as e:
                    enum = "BC162"
                    emsg = "getconn(): [" + str(e) + "]"
                    if self.cfg.tracking:
                        self.log.track(_lev + 1, str(enum) + ": " + str(emsg), True)
                    return [False, str(enum), str(emsg)]

                print("LENGTH OF PROCS = ", str(len(procs)))
                # shutdown ssh and socat processes gracefully
                for proc in procs:
                    proc.terminate()
                    proc.poll()
                    proc.wait()
                procs = []
            else:
                if self.cfg.tracking:
                    self.log.track(_lev + 1, "NO IP Comm Connection to Close.", True)

            return [True, None, None]

        # ---------------------------------------------------------------
        # Close the 'Wi-Fi' Wireless Connection.
        # ---------------------------------------------------------------
        elif self.typ == "WiFi":
            if self.cfg.tracking:
                self.log.track(_lev + 1, "Close 'Wi-Fi' Wireless Connection.", True)

            enum = "BC163"
            emsg = "close(): Wi-Fi Comm NOT Implemented Yet."
            if self.cfg.tracking:
                self.log.track(_lev + 1, str(enum), str(emsg), True)
            return [False, str(enum), str(emsg)]

        # ---------------------------------------------------------------
        # Can't Close UNKNOWN Wireless Connection; Return ERROR.
        # ---------------------------------------------------------------
        else:
            enum = "BC164"
            emsg = "close(): UNKNOWN Comm Type [" + str(self.typ) + "]"
            if self.cfg.tracking:
                self.log.track(_lev + 1, str(enum), str(emsg), True)
            return [False, str(enum), str(emsg)]

    # -------------------------------------------------------------------
    # Acquire the Response from a Command.
    # -------------------------------------------------------------------
    def acquire_response(self, command, wait_time=20):
        # if self.cfg.tracking:
        # self.log.track(_lev, "Entering 'acquire_response()' Method .", True)
        # self.log.track(_lev+13, "_lev: " + str(_lev), True)
        # self.log.track(_lev+13, "^typ: " + str(self.typ), True)

        if self.typ == "iridium":
            # if isinstance(command, str):
            #     command = command.encode("utf-8")
            try:
                self.serialport.write(command + b"\r")

            except Exception as e:
                enum = "BC171"
                emsg = str(e)
                if self.cfg.tracking:
                    self.log.errtrack(str(enum), str(emsg))

                # self.close(1)

            message = ""
            timeout = time.time() + wait_time

            while time.time() < timeout:
                try:
                    if self.serialport.in_waiting == 0:
                        time.sleep(1)
                        continue

                    message += self.serialport.readline()

                    # print("Message: ", message)

                    if "OK" in message:
                        start_idx = message.index("\r") + 1
                        end_idx = message.index("OK")
                        message = message[start_idx:end_idx]
                        message = message.strip()
                        return message

                    elif "READY" in message:
                        return True

                except Exception as e:
                    enum = "BC172"
                    emsg = str(e)
                    if self.cfg.tracking:
                        self.log.errtrack(str(enum), str(emsg))

                    break

            return False

        else:
            enum = "BC173"
            emsg = "close(): UNKNOWN Comm Type [" + str(self.typ) + "]"
            if self.cfg.tracking:
                self.log.errtrack(str(enum), str(emsg))
            return [False, str(enum), str(emsg)]

    # -------------------------------------------------------------------
    # Initiate SBD Session - Send MO buffer to the GSS & Receive MT queued at the GSS
    # -------------------------------------------------------------------
    def check_signal_quality(self, _lev):
        if self.cfg.tracking:
            self.log.track(
                _lev, "Entering 'check_signal_quality()' Class Method.", True
            )
            self.log.track(_lev + 13, "_lev: " + str(_lev), True)

        try:
            if self.cfg.tracking:
                self.log.track(_lev, "Checking the Signal Quality.", True)

            signal_strength = self.acquire_response(b"AT+CSQ")
            if signal_strength is not False and signal_strength.startswith(("+CSQ")):
                signal_strength = signal_strength.split("+CSQ:")[1]
                if self.cfg.tracking:
                    self.log.track(
                        _lev + 1, "Sig Qual: [" + signal_strength + "]", True
                    )

                return signal_strength

            else:
                if self.cfg.tracking:
                    self.log.track(_lev + 1, "Sig Qual: [0]", True)
                return 0

        except Exception as e:
            enum = "BC181"
            emsg = str(e)
            if self.cfg.tracking:
                self.log.errtrack(str(enum), str(emsg))
            return [False, str(enum), str(emsg)], None

    # -------------------------------------------------------------------
    # Initiate SBD Session.
    # -------------------------------------------------------------------

    def initiate_sbd(self, _lev, action, num, wait_time=60):
        # Send MO buffer to the GSS & Receive MT queued at the GSS
        # Command:  +SBDIX
        # Response: <MO status>,<MOMSN>,<MT status>,<MTMSN>,<MT length>,<MT queued>

        if self.cfg.tracking:
            self.log.track(_lev, "Entering 'initiate_sbd()' Class Method.", True)
            self.log.track(_lev + 13, "_lev:  " + str(_lev), True)
            self.log.track(_lev + 13, "_act:  " + str(action), True)
            self.log.track(_lev + 13, "_num:  " + str(num), True)
            self.log.track(_lev + 13, "_wait: " + str(wait_time), True)

        if self.typ == "iridium":
            mt_msg_list = []
            mo_sent = False
            mt_received = False
            mt_queued = 0
            count = 1
            # start_time = time.time()
            # timeout = start_time + wait_time
            timeout = time.time() + wait_time

            if self.cfg.tracking:
                self.log.track(_lev + 1, "SBD Session Initiated.", True)

            if action == "send":
                ######################stop multiple connection attempts after success###########################
                while mo_sent == False and num > 0 and time.time() < timeout:
                    #                while (mo_sent == False or mt_received == False or (mt_queued > 0 and num > 0)) and time.time() < timeout:
                    signal_strength = self.check_signal_quality(_lev + 1)

                    if int(signal_strength) >= 1:

                        response = self.acquire_response(b"AT+SBDIX")
                        if response is not False:
                            response = response.strip()
                            response = response.split("+SBDIX:")[1]
                            response = response.split(",")

                            mo_status = int(response[0])
                            mt_status = int(response[2])
                            mt_queued = int(response[5])

                            if self.cfg.tracking:
                                self.log.track(
                                    _lev + 2, "MO Status: " + str(mo_status), True
                                )
                                self.log.track(
                                    _lev + 2, "MT Status: " + str(mt_status), True
                                )
                                self.log.track(
                                    _lev + 2, "MT Queued: " + str(mt_queued), True
                                )

                            mo_sent = self.read_status(
                                _lev + 2, "mo sbd", mo_status, mo_sent
                            )
                            mt_received = self.read_status(
                                _lev + 2, "mt sbd", mt_status, mt_received
                            )

                            if mt_status == 1:
                                mt_msg = self.get_mt_message(_lev + 2)

                                if mt_msg:
                                    mt_msg_list.append(mt_msg)
                                    num -= 1

                            if mt_queued > 0:
                                if self.cfg.tracking:
                                    self.log.track(
                                        _lev + 1,
                                        "MT SBD messages queued to be transferred: "
                                        + str(mt_queued)
                                        + " messages",
                                        True,
                                    )

                            time.sleep(1)
                        else:
                            if self.cfg.tracking:
                                self.log.track(
                                    _lev + 1, "No SBD response received.", True
                                )
                            time.sleep(3)
                            count += 1

                if self.cfg.tracking:
                    self.log.track(_lev + 1, "Number of retry: " + str(count), True)

                if mo_sent or mt_received:
                    return True, mt_msg_list
                    # print mt_msg_list

                else:
                    if self.cfg.tracking:
                        self.log.track(_lev + 1, "SBD Sesion Time Out.", True)

                    return False, mt_msg_list

            elif action == "receive":
                while (
                    mt_received == False or (mt_queued > 0 and num > 0)
                ) and time.time() < timeout:
                    signal_strength = self.check_signal_quality(_lev + 1)

                    if int(signal_strength) > 2:

                        response = self.acquire_response(b"AT+SBDIX")
                        if response is not False:
                            response = response.strip()
                            response = response.split("+SBDIX:")[1]
                            response = response.split(",")

                            mt_status = int(response[2])
                            mt_queued = int(response[5])

                            if self.cfg.tracking:
                                self.log.track(
                                    _lev + 2, "MT Status: " + str(mt_status), True
                                )
                                self.log.track(
                                    _lev + 2, "MT Queued: " + str(mt_queued), True
                                )

                            mt_received = self.read_status(
                                _lev + 1, "mt sbd", mt_status, mt_received
                            )

                            if mt_status == 1:
                                mt_msg = self.get_mt_message(2)

                                if mt_msg:
                                    mt_msg_list.append(mt_msg)
                                    num -= 1

                            if mt_queued > 0:
                                if self.cfg.tracking:
                                    self.log.track(
                                        _lev + 1,
                                        "MT SBD messages queued to be transferred: "
                                        + str(mt_queued)
                                        + " messages",
                                        True,
                                    )

                            time.sleep(1)

                        else:
                            if self.cfg.tracking:
                                self.log.track(
                                    _lev + 1, "No SBD response received.", True
                                )
                            time.sleep(3)
                            count += 1

                if self.cfg.tracking:
                    self.log.track(_lev + 1, "Number of retry: " + str(count), True)

                if mt_received:
                    return True, mt_msg_list

                else:
                    if self.cfg.tracking:
                        self.log.track(_lev + 1, "SBD Sesion Time Out.", True)

                    return False, mt_msg_list

    # -------------------------------------------------------------------
    # Read Iridium Transfer Status from the AT Command Response
    # -------------------------------------------------------------------

    def read_status(self, _lev, status_type, response, current_flag=False):
        if status_type == "mo buffer":
            if self.cfg.tracking:
                self.log.track(_lev, "MO Buffer Status:", True)
            if response == 0:
                if self.cfg.tracking:
                    self.log.track(
                        _lev + 1,
                        "SBD message successfully stored in mobile originated buffer.",
                        True,
                    )
                return True
            elif response == 1:
                if self.cfg.tracking:
                    self.log.track(
                        _lev + 1,
                        "SBD message write timeout. No terminating carriage return was sent within the transfer period of 60 seconds",
                        True,
                    )
                return False
            elif response == 2:
                if self.cfg.tracking:
                    self.log.track(
                        _lev + 1,
                        "SBD message checksum sent from DTE does not match the checksum calculated at the ISU",
                        True,
                    )
                return False
            elif response == 3:
                if self.cfg.tracking:
                    self.log.track(
                        _lev + 1,
                        "SBD message size is not correct. The maximum mobile originated SBD message length is 1960 \
                        bytes for voice-enabled ISUs, 340 bytes for the 9602, 9602-SB, and 9603, and 205 bytes for the 9601. The minimum \
                        mobile originated SBD message length is 1 byte.",
                        True,
                    )
                return False
            else:
                if self.cfg.tracking:
                    self.log.track(
                        _lev + 1,
                        "An error occurred while attempting to write a message to mobile originated buffer.",
                        True,
                    )
                return False

        elif status_type == "mt sbd":
            if self.cfg.tracking:
                self.log.track(_lev, "MT SBD Message Status:", True)
            if response == 0:
                if self.cfg.tracking:
                    self.log.track(
                        _lev + 1,
                        "No MT SBD message to receive from the Iridium Gateway.",
                        True,
                    )
                return True

            elif response == 1:
                if self.cfg.tracking:
                    self.log.track(
                        _lev + 1,
                        "MT SBD message successfully received to MT buffer from the Iridium Gateway.",
                        True,
                    )
                return True

            else:
                if self.cfg.tracking:
                    self.log.track(
                        _lev + 1,
                        "An error occurred while attempting to perform a mailbox check or receive a message from the Iridium Gatewy.",
                        True,
                    )
                return current_flag

        elif status_type == "mo sbd":
            if self.cfg.tracking:
                self.log.track(_lev, "MO SBD Message Status:", True)

            if response == 0:
                if self.cfg.tracking:
                    self.log.track(
                        _lev + 1, "MO message, if any, transferred successfully.", True
                    )
                return True
            elif response == 1:
                if self.cfg.tracking:
                    self.log.track(
                        _lev + 1,
                        "MO message, if any, transferred successfully, but the MT message in the queue was too big to be transferred.",
                        True,
                    )
                return True
            elif response == 2:
                if self.cfg.tracking:
                    self.log.track(
                        _lev + 1,
                        "MO message, if any, transferred successfully, but the requested Location Update was not accepted.",
                        True,
                    )
                return True
            elif 3 <= response <= 4:
                if self.cfg.tracking:
                    self.log.track(
                        _lev + 1,
                        "Reserved, but indicate MO session success if used.",
                        True,
                    )
                return True
            elif 5 <= response <= 8:
                if self.cfg.tracking:
                    self.log.track(
                        _lev + 1,
                        "Reserved, but indicate MO session failure if used.",
                        True,
                    )
            elif response == 10:
                if self.cfg.tracking:
                    self.log.track(
                        _lev + 1,
                        "Iridium Gateway reported that the call did not complete in the allowed time.",
                        True,
                    )
            elif response == 11:
                if self.cfg.tracking:
                    self.log.track(
                        _lev + 1,
                        "MO message queue at the Iridium Gateway is full.",
                        True,
                    )
            elif response == 12:
                if self.cfg.tracking:
                    self.log.track(_lev + 1, "MO message has too many segments.", True)
            elif response == 13:
                if self.cfg.tracking:
                    self.log.track(
                        _lev + 1,
                        "Iridium Gateway reported that the session did not complete.",
                        True,
                    )
            elif response == 14:
                if self.cfg.tracking:
                    self.log.track(_lev + 1, "Invalid segment size.", True)
            elif response == 15:
                if self.cfg.tracking:
                    self.log.track(_lev + 1, "Access is denied.", True)
            elif response == 16:
                if self.cfg.tracking:
                    self.log.track(
                        _lev + 1,
                        "Transceiver has been locked and may not make SBD calls (see +CULK command).",
                        True,
                    )
            elif response == 17:
                if self.cfg.tracking:
                    self.log.track(
                        _lev + 1,
                        "Iridium Gateway not responding (local session timeout).",
                        True,
                    )
            elif response == 18:
                if self.cfg.tracking:
                    self.log.track(_lev + 1, "Connection lost (RF drop).", True)
            elif response == 19:
                if self.cfg.tracking:
                    self.log.track(
                        _lev + 1,
                        "Link failure (A protocol error caused termination of the call).",
                        True,
                    )
            elif 20 <= response <= 31:
                if self.cfg.tracking:
                    self.log.track(
                        _lev + 1, "Reserved, but indicate failure if used.", True
                    )
            elif response == 32:
                if self.cfg.tracking:
                    self.log.track(
                        _lev + 1, "No network service, unable to initiate call.", True
                    )
            elif response == 33:
                if self.cfg.tracking:
                    self.log.track(
                        _lev + 1, "Antenna fault, unable to initiate call.", True
                    )
            elif response == 34:
                if self.cfg.tracking:
                    self.log.track(
                        _lev + 1,
                        "Radio is disabled, unable to initiate call (see *Rn command).",
                        True,
                    )
            elif response == 35:
                if self.cfg.tracking:
                    self.log.track(
                        _lev + 1, "Transceiver is busy, unable to initiate call.", True
                    )
            elif response == 36:
                if self.cfg.tracking:
                    self.log.track(
                        _lev + 1,
                        "Try later, must wait 3 minutes since last registration.",
                        True,
                    )
            elif response == 37:
                if self.cfg.tracking:
                    self.log.track(
                        _lev + 1, "SBD service is temporarily disabled.", True
                    )
            elif response == 38:
                if self.cfg.tracking:
                    self.log.track(
                        _lev + 1,
                        "Try later, traffic management period (see +SBDLOE command).",
                        True,
                    )
            elif 39 <= response <= 63:
                if self.cfg.tracking:
                    self.log.track(
                        _lev + 1, "Reserved, but indicate failure if used.", True
                    )
            elif response == 64:
                if self.cfg.tracking:
                    self.log.track(
                        _lev + 1,
                        "Band violation (attempt to transmit outside permitted frequency band).",
                        True,
                    )
            elif response == 65:
                if self.cfg.tracking:
                    self.log.track(
                        _lev + 1,
                        "PLL lock failure; hardware error during attempted transmit.",
                        True,
                    )
            else:
                if self.cfg.tracking:
                    self.log.track(_lev + 1, "Unknown response. Assume error.", True)

            return current_flag

    # -------------------------------------------------------------------
    # isactive() Class Library Method.
    # -------------------------------------------------------------------

    def isactive(self, _lev):
        if self.cfg.tracking:
            self.log.track(_lev, "Entering isactive() Class Library Method.", True)
            self.log.track(_lev + 13, "_lev:   " + str(_lev), True)
            self.log.track(_lev + 13, "~comms: " + str(self.cfg.comms), True)

        if not self.cfg.comms:
            if self.cfg.tracking:
                self.log.track(_lev + 1, "Comms NOT ACTIVE; Request Ignored.", True)
            return False
        else:
            if self.cfg.tracking:
                self.log.track(_lev + 1, "Comms ACTIVE; Proceed.", True)
            return True
