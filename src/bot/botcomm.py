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
##  Revision:   1.2 2019/03/04  14:10:00
##  Comment:    Initial method design with stubbed return codes.
##  Developer:  
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
##  Revision:   1.1 2019/02/18  12:00:00
##  Comment:    Module Instantiation.
##  Developer:  
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
########################################################################

import os
import socket
from struct import *

########################################################################
##  The Float Communications Class
########################################################################

class BotComm(object):

    #-------------------------------------------------------------------
    # Initiallize the BotComm Class w/useful BotCfg and BotLog objects.
    #-------------------------------------------------------------------
    def __init__(self, _cfg, _log, _lev):
        self.cfg = _cfg
        self.log = _log
        self.con = None
        if self.cfg.tracking:
            self.log.track(_lev, "Created BotComm Class Object: ", True)
            self.log.track(_lev+1, "log: " + str(self.log), True)
            self.log.track(_lev+1, "cfg: " + str(self.cfg), True)
            self.log.track(_lev+1, "lev: " + str(_lev), True)

    #-------------------------------------------------------------------
    # Establish the communications connection.
    #-------------------------------------------------------------------
    def getconn(self, _lev):
        if self.cfg.tracking:
            self.log.track(_lev, "Establish Communcations Connection.", True)
            self.log.track(_lev+1, "_lev: " + str(_lev), True)

        if self.cfg.type == "Ethernet":
            if self.cfg.tracking:
                self.log.track(_lev+1, "type: " + str(self.cfg.type), True)
                self.log.track(_lev+1, "host: " + str(self.cfg.host), True)
                self.log.track(_lev+1, "port: " + str(self.cfg.port), True)

            if self.con == None:
                try:
                    self.con = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    self.con.settimeout(5)
                    self.con.connect((self.cfg.host, self.cfg.port))
                except socket.timeout:
                    enum = "BC111"
                    emsg = "getconn(): [TIMEOUT]"
                    if self.cfg.tracking:
                        self.log.track(_lev+1, str("Connection Failed: TIMEOUT."), True)
                    return [ False, str(enum), str(emsg) ]
                except Exception as e:
                    enum = "BC101"
                    emsg = "getconn(): [" + str(e)  + "]"
                    if self.cfg.tracking:
                        self.log.errtrack(str(enum), str(emsg))
                    return [ False, str(enum), str(emsg) ]

        if self.cfg.tracking:
            self.log.track(_lev, "Connection Established.", True)
        return [ True, None, None ]

    #-------------------------------------------------------------------
    # Receive a Message.
    #-------------------------------------------------------------------
    def recvmsg(self, _lev):
        if self.cfg.tracking:
            self.log.track(_lev, "Receive Message on Established Connection.", True)
            self.log.track(_lev+1, "_lev: " + str(_lev), True)

        return [ True, None, None ]

    #-------------------------------------------------------------------
    # Send a Message.
    #-------------------------------------------------------------------
    def sendmsg(self, _lev, _msg):
        if self.cfg.tracking:
            self.log.track(_lev, "Send Message on Established Connection.", True)
            self.log.track(_lev+1, "_lev: " + str(_lev), True)
            self.log.track(_lev+1, "_msg: " + _msg.encode("hex"), True)

            if self.cfg.type == "Ethernet":
                msg_siz = len(_msg)
                msg_pos = 0
                pkt_num = 0

                if self.cfg.tracking:
                    self.log.track(_lev+1, "type: 'Ethernet'", True)
                    self.log.track(_lev+1, "size: " + str(msg_siz), True)

                if msg_siz > self.cfg.max_msg_size:
                    enum = "BC111"
                    emsg = "getconn(): [Message Larger Than Max Msg Size.]"
                    if self.cfg.tracking:
                        self.log.track(_lev+1, str("Message Larger Than Max Msg Size."), True)
                    return [ False, str(enum), str(emsg) ]

                while True:
                    if self.cfg.tracking:
                        self.log.track(_lev, "Packet #" + str(pkt_num) + ":", True)


                    #pkt_buf = '{"packet": "' + str(pkt_num) + '"}\n'
                    pkt_buf = pack(">H", pkt_num)
                    hdr_len = len(pkt_buf)
                    msg_end = msg_pos + self.cfg.packet_size - hdr_len
                    pkt_buf += _msg[msg_pos:msg_end]
                    if self.cfg.tracking:
                        self.log.track(_lev+1, "Msg Start:   [" + str(msg_pos) + "]", True)
                        self.log.track(_lev+1, "Msg Finish:  [" + str(msg_end) + "]", True)
                        self.log.track(_lev+1, "Pkt Length:  [" + str(len(pkt_buf)) + "]", True)
                        self.log.track(_lev+1, "Pkt Content: [" + pkt_buf.encode("hex") + "]", True)

                    self.con.sendall(pkt_buf)
                    if self.cfg.tracking:
                        self.log.track(_lev+1, "Packet Sent.", True)
                    
                    msg_pos = msg_end
                    pkt_num += 1

                    if msg_pos > msg_siz or msg_pos > self.cfg.max_msg_size:
                        break

            if self.cfg.tracking:
                self.log.track(_lev, "Message Sent.", True)
                self.log.track(_lev, "Sending 'quit' Message.", True)
                
            self.con.sendall("quit")

            return [ True, None, None ]

    #-------------------------------------------------------------------
    # Send a Message.
    #-------------------------------------------------------------------
    def close(self, _lev):
        if self.cfg.tracking:
            self.log.track(_lev, "Close the Communcations Connection.", True)
            self.log.track(_lev+1, "_lev: " + str(_lev), True)

        if self.con != None:
            try:
                self.con.shutdown(socket.SHUT_RDWR)
                self.con.close()
                self.con = None
            except Exception as e:
                enum = "BC105"
                emsg = "getconn(): [" + str(e)  + "]"
                if self.cfg.tracking:
                    self.log.errtrack(str(enum), str(emsg))
                return [ False, str(enum), str(emsg) ]

        if self.cfg.tracking:
            self.log.track(_lev, "Communcations Connection Closed.", True)
        return [ True, None, None ]
