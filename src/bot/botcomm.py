########################################################################
##
##  Module: botcomm.py
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
##  Revision:   1.1 2019/04/26  12:00:00
##  Comment:    Adding check_signal_quality, updating initiate_sbd, and fixing acquire_response.
##  Developer:  Kevin Ramada; University of Washington
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
##  Revision:   1.1 2019/04/01  12:00:00
##  Comment:    Release 4.1; Able to receive downlink messages.
##  Developer:  Kevin Ramada; University of Washington
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
##  Revision:   1.1 2019/02/18  12:00:00
##  Comment:    Module Instantiation.
##  Developer:  Kevin Ramada; University of Washington
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
########################################################################

import os
import time
import serial
from botcfg import BotCfg
from botlog import BotLog

#cfg = BotCfg()
#log = BotLog(cfg, 0, False)   # False indicates bot-send (not bot-recv)
#log.initlog()


########################################################################
##  The Float Communications Class
########################################################################

class BotComm(object):

    #-------------------------------------------------------------------
    # Initiallize the BotComm Class w/useful BotCfg and BotLog objects.
    #-------------------------------------------------------------------
    def __init__(self, _cfg, _log, _typ, _lev):
        self.cfg = _cfg
        self.log = _log
        self.typ = _typ
        if self.cfg.tracking:
            self.log.track(_lev, "Created BotComm Class Object: ", True)
            self.log.track(_lev+1, "^cfg: " + str(self.cfg), True)
            self.log.track(_lev+1, "^log: " + str(self.log), True)
            self.log.track(_lev+1, "^typ: " + str(self.typ), True)
            self.log.track(_lev+1, "_lev: " + str(_lev), True)

        self.serialport = serial.Serial()

    #-------------------------------------------------------------------
    # Establish the communications connection.
    #-------------------------------------------------------------------
    #def getconn(self, _lev, tout, ocnt):
    def getconn(self, _lev):
        if self.cfg.tracking:
            self.log.track(_lev, "Establish Communcations Connection.", True)
            self.log.track(_lev+1, "typ: " + str(self.typ), True)
            self.log.track(_lev+1, "_lev: " + str(_lev), True)

        if self.typ == 'Iridium':
            if self.cfg.tracking:
                self.log.track(_lev+1, "Set Serial Port.", True)


            try:
                self.serialport.port = self.cfg.port
                self.serialport.baudrate = self.cfg.baud
                self.serialport.timeout = self.cfg.tout

                if self.cfg.tracking:
                    self.log.track(_lev+14, "port: " + str(self.serialport.port), True)
                    self.log.track(_lev+14, "baud: " + str(self.serialport.baudrate), True)
                    self.log.track(_lev+14, "tout: " + str(self.serialport.timeout), True)

                if not self.serialport.isOpen():
                    if self.cfg.tracking:
                        self.log.track(_lev+1, "Open Serial Port", True)
                    self.serialport.open()
                    if self.cfg.tracking:
                        self.log.track(_lev+14, "Opened.", True)

                    command = [b'AT+CGMI', b'AT+CGMM', b'AT+CGMR', b'AT+CGSN']

                    isu_name = self.acquire_response(command[0])
                    if self.cfg.tracking:
                        self.log.track(_lev+14, "ISU Name: " + str(isu_name), True)

                    isu_model_number = self.acquire_response(command[1])
                    if self.cfg.tracking:
                        self.log.track(_lev+14, "ISU Model Number: " + str(isu_model_number), True)

                    isu_version = self.acquire_response(command[2])
                    if self.cfg.tracking:
                        self.log.track(_lev+14, "ISU Version:", True)
                        if isu_version:
                            for line in isu_version.split('\n'):
                                if len(line) > 1:
                                    self.log.track(_lev+3, str(line), True)

                    isu_imei = self.acquire_response(command[3])
                    if self.cfg.tracking:
                        self.log.track(_lev+14, "ISU IMEI: " + str(isu_imei), True)
                else:
                    if self.cfg.tracking:
                        self.log.track(_lev+14, "Serial Port ALREADY Connected", True)

                return [ True, None, None ]

            except Exception as e:
                enum = "BC101"
                emsg = str(e)
                if self.cfg.tracking:
                    self.log.errtrack(str(enum), str(emsg))
                return [ False, str(enum), str(emsg) ]

    #-------------------------------------------------------------------
    # Receive a Message.
    #-------------------------------------------------------------------
    def receive(self, _lev, num):
        if self.cfg.tracking:
            self.log.track(_lev, "Receive Message on Established Connection.", True)
            self.log.track(_lev+1, "_lev: " + str(_lev), True)

        if self.typ == 'Iridium':
            try:
                response = self.acquire_response(b'AT+SBDWT')
                if response == True:
                    if self.cfg.tracking:
                        self.log.track(_lev, "SBD Modem Ready To Receive Message", True)

                    response = self.acquire_response("" + '\r')
                    mo_buffer = self.read_status(_lev, 'mo buffer', int(response))

                    if mo_buffer == True:
                        cnc_msgs = self.initiate_sbd(_lev, "receive", num)
                        if self.cfg.tracking:
                            self.log.track(_lev+1, "All messages received: " + cnc_msgs, True)

                        return [ True, None, None ], cnc_msgs

                    else:
                        enum = "BC107"
                        emsg = "Mailbox check couldn't be sent to the mobile originated buffer."
                        if self.cfg.tracking:
                            self.log.errtrack(str(enum), str(emsg))

                        return [ False, str(enum), str(emsg) ], None

                else:
                    enum = "BC108"
                    emsg = "Connection Error. SBD modem didn't get any response from the mailbox check."
                    if self.cfg.tracking:
                        self.log.errtrack(str(enum), str(emsg))

                    return [ False, str(enum), str(emsg) ], None

            except Exception as e:
                enum = "BC109"
                emsg = str(e)
                if self.cfg.tracking:
                    self.log.errtrack(str(enum), str(emsg))
                return [ False, str(enum), str(emsg)], None


    #-------------------------------------------------------------------
    # Get a MT Message from the MT buffer
    #-------------------------------------------------------------------
    def get_mt_message(self, _lev):
        try:
            if self.cfg.tracking:
                self.log.track(_lev, "Attempting Message Read(s).", True)

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

            response = self.acquire_response(b'AT+SBDRT')
            if response is False:
                enum = "BC114"
                emsg = "Connection Error. MT Message couldn't be transferred."
                if self.cfg.tracking:
                    self.log.errtrack(str(enum), str(emsg))
            else:
                response = response.split("+SBDRT:\r\n")[1]
                if self.cfg.tracking:
                    self.log.track(_lev+1, "Message: " + str(response).encode('hex'), True)
            return response

            # return [ True, None, None ], results

        except Exception as e:
            enum = "BC115"
            emsg = str(e)
            if self.cfg.tracking:
                self.log.errtrack(str(enum), str(emsg))
            return [ False, str(enum), str(emsg)], None

    #-------------------------------------------------------------------
    # Send a Message.
    #-------------------------------------------------------------------
    def send(self, _lev, msg, num):
        if self.cfg.tracking:
            self.log.track(_lev, "Send Message on Established Connection.", True)
            self.log.track(_lev+1, "_lev: " + str(_lev), True)
            self.log.track(_lev+1, "msg: " + str(msg).encode("hex"), True)

        if self.typ == 'Iridium':
            try:
                response = self.acquire_response(b'AT+SBDWT')
                if response == True:
                    if self.cfg.tracking:
                        self.log.track(_lev, "SBD Modem Ready To Receive Message", True)

                    response = self.acquire_response(msg)
                    # print response
                    mo_buffer = self.read_status(_lev, 'mo buffer', int(response))

                    if mo_buffer == True:
                        success, cnc_msgs = self.initiate_sbd(_lev, "send", num)
                        if self.cfg.tracking:
                            self.log.track(_lev+1, "All messages received: " + cnc_msgs, True)

                        if success:
                            return [ True, None, None ], cnc_msgs
                        else:
                            enum = "BC110"
                            emsg = "SBD Sesion Time Out"
                            if self.cfg.tracking:
                                self.log.errtrack(str(enum), str(emsg))

                            return [ False, str(enum), str(emsg) ], None

                    else:
                        enum = "BC104"
                        emsg = "MO Message couldn't be sent to the mobile originated buffer."
                        if self.cfg.tracking:
                            self.log.errtrack(str(enum), str(emsg))

                        return [ False, str(enum), str(emsg) ], None

                else:
                    enum = "BC105"
                    emsg = "Connection Error. SBD modem didn't get any response."
                    if self.cfg.tracking:
                        self.log.errtrack(str(enum), str(emsg))

                    return [ False, str(enum), str(emsg) ], None


            except Exception as e:
                enum = "BC106"
                emsg = str(e)
                if self.cfg.tracking:
                    self.log.errtrack(str(enum), str(emsg))
                return [ False, str(enum), str(emsg) ], None

    #-------------------------------------------------------------------
    # Close the Connection.
    #-------------------------------------------------------------------
    def close(self, _lev):
        if self.cfg.tracking:
            self.log.track(_lev, "Close the Communications Connection.", True)
            self.log.track(_lev+1, "_lev: " + str(_lev), True)
            self.log.track(_lev+1, "^typ: " + str(self.typ), True)

        if self.typ == 'Iridium':
            try:
                if self.serialport.isOpen():
                    self.serialport.close()
                    if self.cfg.tracking:
                        self.log.track(_lev+1, "Serial Port Closed.", True)
                else:
                    if self.cfg.tracking:
                        self.log.track(_lev+1, "Serial Port Already Closed.", True)

                return [ True, None, None ]

            except Exception as e:
                enum = "BC111"
                emsg = str(e)
                if self.cfg.tracking:
                    self.log.errtrack(str(enum), str(emsg))
                return [ False, str(enum), str(emsg) ]


    #-------------------------------------------------------------------
    # Acquire the Response from a Command.
    #-------------------------------------------------------------------
    def acquire_response(self, command, wait_time=20):
        if self.typ == 'Iridium':
            # if isinstance(command, str):
            #     command = command.encode("utf-8")

            try:
                self.serialport.reset_input_buffer()
                self.serialport.write(command + b'\r')

            except Exception as e:
                enum = "BC112"
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
                        start_idx = message.index('\n') + 1
                        end_idx = message.index("OK")
                        message = message[start_idx:end_idx]
                        message = message.strip()
                        return message

                    elif "READY" in message:
                        return True

                except Exception, e:
                    enum = "BC113"
                    emsg = str(e)
                    if self.cfg.tracking:
                        self.log.errtrack(str(enum), str(emsg))

                    break

            return False


    #-------------------------------------------------------------------
    # Initiate SBD Session - Send MO buffer to the GSS & Receive MT queued at the GSS
    #-------------------------------------------------------------------
    def check_signal_quality(self, _lev):
        try:
            if self.cfg.tracking:
                self.log.track(_lev, "Checking the signal quality.", True)

            signal_strength = self.acquire_response(b"AT+CSQ")
            if signal_strength is not False:
                signal_strength = signal_strength.split('+CSQ:')[1]
                if self.cfg.tracking:
                    self.log.track(_lev, "Signal strength: " + signal_strength, True)

                return signal_strength

        except Exception as e:
            enum = "BC116"
            emsg = str(e)
            if self.cfg.tracking:
                self.log.errtrack(str(enum), str(emsg))
            return [ False, str(enum), str(emsg)], None


    #-------------------------------------------------------------------
    # Initiate SBD Session - Send MO buffer to the GSS & Receive MT queued at the GSS
    #-------------------------------------------------------------------
    def initiate_sbd(self, _lev, action, num, wait_time=1000):
    # Response: +SBDIX:<MO status>,<MOMSN>,<MT status>,<MTMSN>,<MT length>,<MT queued>

        if self.typ == 'Iridium':
            mt_msg_list = []
            mo_sent = False
            mt_received = False
            mt_queued = 0
            count = 1
            start_time = time.time()
            timeout = start_time + wait_time

            if self.cfg.tracking:
                self.log.track(_lev, "SBD Session Initiated.", True)

            if action == 'send':
                while (mo_sent == False or mt_received == False or (mt_queued > 0 and num > 0)) and start_time < timeout:
                    signal_strength = self.check_signal_quality(2)

                    if int(signal_strength) > 2:

                        response = self.acquire_response(b"AT+SBDIX")
                        if response is not False:
                            response = response.strip()
                            response = response.split('+SBDIX:')[1]
                            response = response.split(',')

                            mo_status = int(response[0])
                            mt_status = int(response[2])
                            mt_queued = int(response[5])

                            if self.cfg.tracking:
                                self.log.track(_lev+1, "MO Status: " + str(mo_status), True)
                                self.log.track(_lev+1, "MT Status: " + str(mt_status), True)
                                self.log.track(_lev+1, "MT Queued: " + str(mt_queued), True)

                            mo_sent = self.read_status(_lev+1, 'mo sbd', mo_status)
                            mt_received = self.read_status(_lev+1, 'mt sbd', mt_status)

                            if mt_status == 1:
                                mt_msg = self.get_mt_message(2)

                                if mt_msg:
                                    mt_msg_list.append(mt_msg)
                                    num -= 1

                            if mt_queued > 0:
                                if self.cfg.tracking:
                                    self.log.track(_lev+1, "MT SBD messages queued to be transferred: " + str(mt_queued) + " messages", True)

                            time.sleep(5)
                        else:
                            if self.cfg.tracking:
                                self.log.track(_lev+1, "No SBD response received.", True)
                            time.sleep(5)
                            count += 1
                if self.cfg.tracking:
                    self.log.track(_lev+1, "Number of retry: " + str(count), True)
                return True, mt_msg_list
                # print mt_msg_list

            elif action == 'receive':
                while (mt_received == False or (mt_queued > 0 and num > 0)) and start_time < timeout:
                    signal_strength = self.check_signal_quality(2)

                    if int(signal_strength) > 2:

                        response = self.acquire_response(b"AT+SBDIX")
                        if response is not False:
                            response = response.strip()
                            response = response.split('+SBDIX:')[1]
                            response = response.split(',')

                            mt_status = int(response[2])
                            mt_queued = int(response[5])

                            if self.cfg.tracking:
                                self.log.track(_lev+1, "MT Status: " + str(mt_status), True)
                                self.log.track(_lev+1, "MT Queued: " + str(mt_queued), True)

                            mt_received = self.read_status(_lev+1, 'mt sbd', mt_status)

                            if mt_status == 1:
                                mt_msg = self.get_mt_message(2)

                                if mt_msg:
                                    mt_msg_list.append(mt_msg)
                                    num -= 1

                            if mt_queued > 0:
                                if self.cfg.tracking:
                                    self.log.track(_lev+1, "MT SBD messages queued to be transferred: " + str(mt_queued) + " messages", True)

                            time.sleep(5)

                        else:
                            if self.cfg.tracking:
                                self.log.track(_lev+1, "No SBD response received.", True)
                            time.sleep(5)
                            count += 1

                if self.cfg.tracking:
                    self.log.track(_lev+1, "Number of retry: " + str(count), True)
                return mt_msg_list


    #-------------------------------------------------------------------
    # Read Iridium Transfer Status from the AT Command Response
    #-------------------------------------------------------------------

    def read_status(self, _lev, status_type, response):
        if status_type == 'mo buffer':
            if self.cfg.tracking:
                self.log.track(_lev, "MO Buffer Status:", True)
            if response == 0:
                if self.cfg.tracking:
                    self.log.track(_lev+1, "SBD message successfully stored in mobile originated buffer.", True)
                return True
            elif response == 1:
                if self.cfg.tracking:
                    self.log.track(_lev+1, "SBD message write timeout. No terminating carriage return was sent within the transfer period of 60 seconds", True)
                return False
            else:
                if self.cfg.tracking:
                    self.log.track(_lev+1, "An error occurred while attempting to write a message to mobile originated buffer.", True)
                return False

        elif status_type == 'mt sbd':
            if self.cfg.tracking:
                self.log.track(_lev, "MT SBD Message Status:", True)
            if response == 0:
                if self.cfg.tracking:
                    self.log.track(_lev+1, "No MT SBD message to receive from the Iridium Gateway.", True)
                return True

            elif response == 1:
                if self.cfg.tracking:
                    self.log.track(_lev+1, "MT SBD message successfully received to MT buffer from the Iridium Gateway.", True)
                return True

            else:
                if self.cfg.tracking:
                    self.log.track(_lev+1, "An error occurred while attempting to perform a mailbox check or receive a message from the Iridium Gateway.", True)
                return False

        elif status_type == 'mo sbd':
            if self.cfg.tracking:
                self.log.track(_lev, "MO SBD Message Status:", True)

            if response == 0:
                if self.cfg.tracking:
                    self.log.track(_lev+1, "MO message, if any, transferred successfully.", True)
                return True
            elif response == 1:
                if self.cfg.tracking:
                    self.log.track(_lev+1, "MO message, if any, transferred successfully, but the MT message in the queue was too big to be transferred.", True)
                return True
            elif response == 2:
                if self.cfg.tracking:
                    self.log.track(_lev+1, "MO message, if any, transferred successfully, but the requested Location Update was not accepted.", True)
                return True
            elif  3 <= response <= 4:
                if self.cfg.tracking:
                    self.log.track(_lev+1, "Reserved, but indicate MO session success if used.", True)
                return True
            elif  5 <= response <= 8:
                if self.cfg.tracking:
                    self.log.track(_lev+1, "Reserved, but indicate MO session failure if used.", True)
            elif response == 10:
                if self.cfg.tracking:
                    self.log.track(_lev+1, "Iridium Gateway reported that the call did not complete in the allowed time.", True)
            elif response == 11:
                if self.cfg.tracking:
                    self.log.track(_lev+1, "MO message queue at the Iridium Gateway is full.", True)
            elif response == 12:
                if self.cfg.tracking:
                    self.log.track(_lev+1, "MO message has too many segments.", True)
            elif response == 13:
                if self.cfg.tracking:
                    self.log.track(_lev+1, "Iridium Gateway reported that the session did not complete.", True)
            elif response == 14:
                if self.cfg.tracking:
                    self.log.track(_lev+1, "Invalid segment size.", True)
            elif response == 15:
                if self.cfg.tracking:
                    self.log.track(_lev+1, "Access is denied.", True)
            elif response == 16:
                if self.cfg.tracking:
                    self.log.track(_lev+1, "Transceiver has been locked and may not make SBD calls (see +CULK command).", True)
            elif response == 17:
                if self.cfg.tracking:
                    self.log.track(_lev+1, "Iridium Gateway not responding (local session timeout).", True)
            elif response == 18:
                if self.cfg.tracking:
                    self.log.track(_lev+1, "Connection lost (RF drop).", True)
            elif response == 19:
                if self.cfg.tracking:
                    self.log.track(_lev+1, "Link failure (A protocol error caused termination of the call).", True)
            elif 20 <= response <= 31:
                if self.cfg.tracking:
                    self.log.track(_lev+1, "Reserved, but indicate failure if used.", True)
            elif response == 32:
                if self.cfg.tracking:
                    self.log.track(_lev+1, "No network service, unable to initiate call.", True)
            elif response == 33:
                if self.cfg.tracking:
                    self.log.track(_lev+1, "Antenna fault, unable to initiate call.", True)
            elif response == 34:
                if self.cfg.tracking:
                    self.log.track(_lev+1, "Radio is disabled, unable to initiate call (see *Rn command).", True)
            elif response == 35:
                if self.cfg.tracking:
                    self.log.track(_lev+1, "Transceiver is busy, unable to initiate call.", True)
            elif response == 36:
                if self.cfg.tracking:
                    self.log.track(_lev+1, "Try later, must wait 3 minutes since last registration.", True)
            elif response == 37:
                if self.cfg.tracking:
                    self.log.track(_lev+1, "SBD service is temporarily disabled.", True)
            elif response == 38:
                if self.cfg.tracking:
                    self.log.track(_lev+1, "Try later, traffic management period (see +SBDLOE command).", True)
            elif 39 <= response <= 63:
                if self.cfg.tracking:
                    self.log.track(_lev+1, "Reserved, but indicate failure if used.", True)
            elif response == 64:
                if self.cfg.tracking:
                    self.log.track(_lev+1, "Band violation (attempt to transmit outside permitted frequency band).", True)
            elif response == 65:
                if self.cfg.tracking:
                    self.log.track(_lev+1, "PLL lock failure; hardware error during attempted transmit.", True)
            else:
                if self.cfg.tracking:
                    self.log.track(_lev+1, "Unknown response. Assume error.", True)

            return False
