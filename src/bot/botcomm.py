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
    def getconn(self, lev):
        if self.cfg.tracking:
            self.log.track(lev, "Establish Communcations Connection.", True)
            self.log.track(lev+1, "typ: " + str(self.typ), True)
            self.log.track(lev+1, "lev: " + str(lev), True)

        if self.typ == 'Iridium':
            if self.cfg.tracking:
                self.log.track(lev+1, "Set Serial Port.", True)
            

            try:
                self.serialport.port = "/dev/ttyUSB0"
                self.serialport.baudrate = 19200
                self.serialport.timeout = 1

                if self.cfg.tracking:
                    self.log.track(lev+2, "port: " + str(self.serialport.port), True)
                    self.log.track(lev+2, "baud: " + str(self.serialport.baudrate), True)
                    self.log.track(lev+2, "tout: " + str(self.serialport.timeout), True)

                if not self.serialport.isOpen():
                    if self.cfg.tracking:
                        self.log.track(lev+1, "Open Serial Port", True)
                    self.serialport.open()
                    if self.cfg.tracking:
                        self.log.track(lev+2, "Opened.", True)

                    command = [b'AT+CGMI', b'AT+CGMM', b'AT+CGMR', b'AT+CGSN']
                    
                    isu_name = self.acquire_response(command[0])
                    if self.cfg.tracking:
                        self.log.track(lev+2, "ISU Name: " + isu_name, True)
                    
                    isu_model_number = self.acquire_response(command[1])
                    if self.cfg.tracking:
                        self.log.track(lev+2, "ISU Model Number: " + isu_model_number, True)
                    
                    isu_version = self.acquire_response(command[2])
                    if self.cfg.tracking:
                        self.log.track(lev+2, "ISU Version:", True)
                        for line in isu_version.split('\n'):
                            if len(line) > 1:
                                self.log.track(lev+3, str(line), True)
                    
                    isu_imei = self.acquire_response(command[3])
                    if self.cfg.tracking:
                        self.log.track(lev+2, "ISU IMEI: " + isu_imei, True)
                else:
                    if self.cfg.tracking:
                        self.log.track(lev+1, "Serial Port ALREADY Connected", True)

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
    def receive(self, _lev):
        if self.cfg.tracking:
            self.log.track(_lev, "Receive Message on Established Connection.", True)
            self.log.track(_lev+1, "_lev: " + str(_lev), True)

        if self.typ == 'Iridium':
            try:
                if self.cfg.tracking:
                    self.log.track(_lev+1, "Attempting Message Read(s).", True)
                icnt = 0
                results = []
                tries = 0
                while True:
                    iline = self.serialport.readline()
                    if self.cfg.tracking:
                        self.log.track(_lev+2, "iline " + str(tries) + ":" + str(iline), True)
                    if iline.strip() == "":
                        if tries > 5:
                            if self.cfg.tracking:
                                self.log.track(_lev+2, "DONE.", True)
                            break
                        else:
                            tries = tries + 1
                    else:
                        results.append(iline)
                        icnt = icnt + 1
                
                return [ True, None, None ], results

            except Exception as e:
                enum = "BC102"
                emsg = str(e)
                if self.cfg.tracking:
                    self.log.errtrack(str(enum), str(emsg))
                return [ False, str(enum), str(emsg)], None

    #-------------------------------------------------------------------
    # Send a Message.
    #-------------------------------------------------------------------
    def send(self, lev, msg):
        if self.cfg.tracking:
            self.log.track(lev, "Send Message on Established Connection.", True)
            self.log.track(lev+1, "lev: " + str(lev), True)
            self.log.track(lev+1, "msg: " + str(msg), True)

        if self.typ == 'Iridium':
            try:
                response = self.acquire_response(b'AT+SBDWT')
                if response == True:
                    self.log.track(lev, "SBD Modem Ready To Receive Message", True)

                    #response = self.acquire_response(sbdmsg + '\r')
                    mo_buffer = self.read_status(lev, 'mo buffer', int(response))
                    
                    if mo_buffer == True:
                        self.initiate_sbd(lev)
                else:
                    print "Connection Error. SBD Modem Cannot Receive Message"


                ### Condition for +SBDWB
                # msg = self.acquire_response(b'AT+SBDWB={message size}')
                # elif msg == '3':
                #     print "SBD message size is not correct. It should be between 1-340 bytes"
                ###

            except Exception as err:
                print "Error: ", err

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

            except Exception as e:
                enum = "BC111"
                emsg = str(e)
                if self.cfg.tracking:
                    self.log.errtrack(str(enum), str(emsg))
                return [ False, str(enum), str(emsg) ]


    #-------------------------------------------------------------------
    # Acquire the Response from a Command.
    #-------------------------------------------------------------------    
    def acquire_response(self, command, wait_time=10):
        if self.typ == 'Iridium':
            # if isinstance(command, str):
            #     command = command.encode("utf-8")

            try:
                self.serialport.reset_input_buffer()
                self.serialport.write(command + b'\r')
            
            except Exception as err:
                print "Error: ", err
                self.close(1)

            message = ""
            timeout = time.time() + wait_time

            while time.time() < timeout:
                try:
                    if self.serialport.in_waiting == 0:
                        time.sleep(1)
                        continue

                    message += self.serialport.readline()

                    # print("Message:", message)

                    if "OK" in message:
                        if command in message:
                            start_idx = message.index('\n') + 1
                            end_idx = message.index("OK") - 4
                            message = message[start_idx:end_idx]
                            return message
                        return True
                    elif "READY" in message :
                        return True

                except Exception, err:
                    print "Error: ", err
                    break

            return False


    #-------------------------------------------------------------------
    # Initiate SBD Session - Send MO buffer to the GSS & Receive MT queued at the GSS
    #-------------------------------------------------------------------  
    def initiate_sbd(self, lev):
    # Response: +SBDIX:<MO status>,<MOMSN>,<MT status>,<MTMSN>,<MT length>,<MT queued>

        if self.typ == 'Iridium':
            mt_msg_list = []
            mo_sent = False
            mt_received = False
            mt_queued = 0
            count = 1

            while mo_sent == False or mt_received == False or mt_queued > 0:

                response = self.acquire_response(b"AT+SBDIX\r")

                if response is not False:

                    response = response.strip() 
                    response = response.split(':')[1]
                    response = response.split(',')  

                    mo_status = int(response[0])
                    mt_status = int(response[2])
                    mt_queued = int(response[5])

                    print "MO Status: " + str(mo_status)
                    print "MT Status: " + str(mt_status)
                    print "MT Queued: " + str(mt_queued)

                    mo_sent = self.read_status(lev, 'mo sbd', mo_status)

                    if mt_status == 0:     
                        self.log.track(lev, "MT SBD Message Status:", True)
                        self.log.track(lev+1, "No MT SBD message to receive from the Iridium Gateway.", True)
                        mt_received = True
                    
                    elif mt_status == 1:      
                        self.log.track(lev, "MT SBD Message Status:", True)
                        self.log.track(lev+1, "MT SBD message successfully received to MT buffer from the Iridium Gateway.", True)
                        mt_msg = self.receive(2)

                        #if mtMsg:
                        if mt_msg:
                            mt_msg_list.append(mt_msg)

                        mt_received = True

                    else:
                        self.log.track(lev, "MT SBD Message Status:", True)
                        self.log.track(lev+1, "An error occurred while attempting to perform a mailbox check or receive a message from the Iridium Gatewy.", True)
                        mt_received = False

                    if mt_queued > 0:
                        self.log.track(lev, "MT SBD messages queued to be transferred: " + mt_queued + " messages", True)

                    # time.sleep(5)
                else:
                    self.log.track(lev, "No SBD response received.", True)
                    count += 1
                    # time.sleep(3)
            print "Number of retry: " + str(count)
            # return response


    #-------------------------------------------------------------------
    # Read Iridium Transfer Status from the AT Command Response 
    #-------------------------------------------------------------------  

    def read_status(self, lev, status_type, response):
        if status_type == 'mo buffer':
            self.log.track(lev, "MO Buffer Status:", True)
            if response == 0:
                self.log.track(lev+1, "SBD message successfully stored in mobile originated buffer.", True)
                return True
            elif response == 1:
                self.log.track(lev+1, "SBD message write timeout. No terminating carriage return was sent within the transfer period of 60 seconds", True)
                return False


        elif status_type == 'mo sbd':
            self.log.track(lev, "MO SBD Message Status:", True)
            if response == 0:
                self.log.track(lev+1, "MO message, if any, transferred successfully.", True)
                return True
            elif response == 1:
                self.log.track(lev+1, "MO message, if any, transferred successfully, but the MT message in the queue was too big to be transferred.", True)
                return True
            elif response == 2:
                self.log.track(lev+1, "MO message, if any, transferred successfully, but the requested Location Update was not accepted.", True)
                return True
            elif  3 <= response <= 4:
                self.log.track(lev+1, "Reserved, but indicate MO session success if used.", True)
                return True
            elif  5 <= response <= 8:
                self.log.track(lev+1, "Reserved, but indicate MO session failure if used.", True)
            elif response == 10:
                self.log.track(lev+1, "Iridium Gateway reported that the call did not complete in the allowed time.", True)
            elif response == 11:
                self.log.track(lev+1, "MO message queue at the Iridium Gateway is full.", True)
            elif response == 12:
                self.log.track(lev+1, "MO message has too many segments.", True)
            elif response == 13:
                self.log.track(lev+1, "Iridium Gateway reported that the session did not complete.", True)
            elif response == 14:
                self.log.track(lev+1, "Invalid segment size.", True)
            elif response == 15:
                self.log.track(lev+1, "Access is denied.", True)
            elif response == 16:
                self.log.track(lev+1, "Transceiver has been locked and may not make SBD calls (see +CULK command).", True)
            elif response == 17:
                self.log.track(lev+1, "Iridium Gateway not responding (local session timeout).", True)
            elif response == 18:
                self.log.track(lev+1, "Connection lost (RF drop).", True)
            elif response == 19:
                self.log.track(lev+1, "Link failure (A protocol error caused termination of the call).", True)
            elif 20 <= response <= 31:
                self.log.track(lev+1, "Reserved, but indicate failure if used.", True)
            elif response == 32:
                self.log.track(lev+1, "No network service, unable to initiate call.", True)
            elif response == 33:
                self.log.track(lev+1, "Antenna fault, unable to initiate call.", True)
            elif response == 34:
                self.log.track(lev+1, "Radio is disabled, unable to initiate call (see *Rn command).", True)
            elif response == 35:
                self.log.track(lev+1, "Transceiver is busy, unable to initiate call.", True)
            elif response == 36:
                self.log.track(lev+1, "Try later, must wait 3 minutes since last registration.", True)
            elif response == 37:
                self.log.track(lev+1, "SBD service is temporarily disabled.", True)
            elif response == 38:
                self.log.track(lev+1, "Try later, traffic management period (see +SBDLOE command).", True)
            elif 39 <= response <= 63:
                self.log.track(lev+1, "Reserved, but indicate failure if used.", True)
            elif response == 64:
                self.log.track(lev+1, "Band violation (attempt to transmit outside permitted frequency band).", True)
            elif response == 65:    
                self.log.track(lev+1, "PLL lock failure; hardware error during attempted transmit.", True)
            else:
                self.log.track(lev+1, "Unknown response. Assume error.", True)

            return False
