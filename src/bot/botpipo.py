########################################################################
##
##  Module: botpipo.py
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
##  Revision:   1.2 2019/02/13  13:25:00
##  Comment:    Upgrade to latest Config and Logging Model.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
##  Revision:   1.1 2019/02/06  15:40:00
##  Comment:    Module Instantiation.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
########################################################################

import os
#import sys
import math
import time 
from bothelp import readFloatFile

########################################################################
##  The Float PIPO Class
########################################################################

class BotPIPO(object):

    def __init__(self, cfg, log):
        self.cfg = cfg
        self.log = log
        self.scor = self.cfg.pipo_scor_wt
        self.qual = self.cfg.pipo_qual_wt
        self.size = self.cfg.pipo_size_wt
        self.trig = self.cfg.pipo_trig_wt
        self.time = self.cfg.pipo_time_wt
        self.purg = self.cfg.purge_rating
        if self.cfg.tracking:
            self.log.track(0, 'Creating PIPO Class Object.', True)
            self.log.track(1, 'Configuring PIPO Class Object.', True)
            self.log.track(2, "scor = " + str(self.scor), True)
            self.log.track(2, "qual = " + str(self.qual), True)
            self.log.track(2, "size = " + str(self.size), True)
            self.log.track(2, "trig = " + str(self.trig), True)
            self.log.track(2, "time = " + str(self.time), True)
            self.log.track(2, "purg = " + str(self.purg), True)

    #---------------------------------------------------------------
    # getpipo() class module.
    #---------------------------------------------------------------
    # Calculate the PIPO Rating for a Data Product.

    def getPipo(self, lev, metafile, metajson, trigger):
        if self.cfg.tracking:
            self.log.track(lev, "Entering 'getpipo()' Module.", True)
            self.log.track(lev+1, "Log Level: [" + str(lev) + "]", True)
            self.log.track(lev+1, "Meta File: [" + str(metafile) + "]", True)
            self.log.track(lev+6, "Meta JSON: [" + str(metajson) + "]", True)
            self.log.track(lev+1, "Trigger:   [" + str(trigger) + "]", True)

        try:
            datafolder = os.path.dirname(metafile)
            if self.cfg.tracking:
                self.log.track(lev, "Compute Data Folder from Meta File.", True)
                self.log.track(lev+1, "Data Folder: " + str(datafolder), True)
        except:
            return [ False, 1,  "CAN'T EXTRACT DATA FOLDER." ], None

        #-----------------------------------------------------------
        # Compute 'Standard' Data Product Size.
        if self.cfg.tracking:
            self.log.track(lev, "Compute 'Standard' Data File size.", True)

        sfile_size = 0
        stdfile = ""
        try:
            sf = str(metajson['data_file'])
        except:
            return [ False, 1,  "MANDATORY 'STANDARD' DATA FILE KEYWORD MISSING." ], None

        if sf == "":
            return [ False, 2, "MANDATORY 'STANDARD' DATA FILE NAME MISSING." ], None

        sf = datafolder + "/" + sf

        try:
            if os.path.isfile(sf):
                sfile_size = os.path.getsize(sf)
            else:
                return [ False, 3, "MANDATORY 'STANDARD' DATA FILE NOT ACCESSIBLE." ]
        except:
            if self.cfg.tracking:
                self.log.errtrack("PIPO101", "CAN'T ACCESS 'STANDARD' DATA FILE.", True)
            return [ False, "PIPO101", "CAN'T ACCESS 'STANDARD' DATA FILE."], None

        acquired, stdfile = readFloatFile(sf, False, False)
        if not acquired:
            if self.cfg.tracking:
                self.log.errtrack("PIPO111", "CAN'T ACQUIRE 'STANDARD' DATA FILE.", True)
            return [ False, "PIPO111", "CAN'T ACQUIRE 'STANDARD' DATA FILE."], None
            
        if self.cfg.tracking:
            self.log.track(lev+1, "Std File: " + str(sf), True)
            self.log.track(lev+1, "Std Size: " + str(sfile_size), True)

        #-----------------------------------------------------------
        # Compute 'Change' Data Product Size.
        if self.cfg.tracking:
            self.log.track(lev, "Compute 'Change' Data File size.", True)

        cfile_size = 0
        chgfile = ""
        try:
            cf = str(metajson['change_file'])
        except:
            cf = None

        if not cf:
            if self.cfg.tracking:
                self.log.track(lev+2, "No Change File Keyword Found; Continue.", True)
        else:
            if self.cfg.tracking:
                self.log.track(lev+2, "Chg File: " + str(cf), True)

            cfile_size = 0
            if cf != "NC":
                cf = datafolder + "/" + cf 
                if os.path.isfile(cf):
                    try:
                        cfile_size = os.path.getsize(cf)
                    except:
                        cfile_size = 0
                        self.log.track(lev+2, "OPTIONAL 'CHANGE' DATA FILE NOT ACCESSIBLE.", True)

                    if cfile_size > 0:
                        acquired, chgfile = readFloatFile(cf, False, False)
                        if not acquired:
                            if self.cfg.tracking:
                                self.log.errtrack("PIPO112", "CAN'T ACQUIRE 'CHANGE' DATA FILE.", True)
                            return [ False, "PIPO112", "CAN'T ACQUIRE 'CHANGE' DATA FILE."], None  
                else:
                    self.log.track(lev+2, False, "OPTIONAL 'CHANGE' DATA FILE NOT A FILE.", True)

            if self.cfg.tracking:
                self.log.track(lev+2, "Chg Size: " + str(cfile_size), True)
    
        #-----------------------------------------------------------
        # get Normalized Data Product Size.
        if self.cfg.tracking:
            self.log.track(lev, "Get 'Normalized' Data Product size.", True)

        success, norm = self.computeNormalized(lev+1, sfile_size, cfile_size)

        # Calculation of the PIPO file size 'normalization' should
        # NEVER fail; it may however calculate that the normaized
        # size is too big for transmission, rendering it ineligible.
        dstate = 0
        if not success:
            dstate = 3      # State of '3' indicates 'Ineligible'

        #-----------------------------------------------------------
        # Get PIPO Numerator.
        if self.cfg.tracking:
            self.log.track(lev, "Get PIPO Formula Numerator.", True)
        
        success, numerator = self.computeNumerator(lev+1, float(metajson["node_id_score"]), float(metajson["quality"]), norm, trigger)

        # Calculation of the PIPO Numerator should NEVER fail; it's
        # a simple multiplier and adder calculation.  If it does,
        # the method returns 0.0, but guarantee this anyway.
        if not success:
            numerator = 0.0

        #-----------------------------------------------------------
        # Compute PIPO Denominator.
        if self.cfg.tracking:
            self.log.track(lev, "Get PIPO Formula Denominator.", True)

        success, denominator = self.computeDenominator(lev+1, metafile)

        # Calculation of the PIPO Denominator should NEVER fail; it's
        # a simple multiplier and adder calculation.  If it does,
        # the method returns 1.0, but guarantee this anyway.
        if not success:
            denominator = 1.0

        #-----------------------------------------------------------
        # Compute PIPO Rating.
        if self.cfg.tracking:
            self.log.track(lev, "Computing PIPO Rating.", True)

        pipo_rating = numerator / denominator
        if self.cfg.tracking:
            self.log.track(lev+1, "PIPO Rating: " + str(pipo_rating), True)

        return [ True, None, None], [ numerator, sfile_size, cfile_size, dstate, norm, pipo_rating, stdfile, chgfile ]

    #---------------------------------------------------------------
    # computeNormalized() class module.
    #---------------------------------------------------------------
    def computeNormalized(self, lev, std, chg):
        if self.cfg.tracking:
            self.log.track(lev, "Compute 'Normalized' Data Product size.", True)
            self.log.track(lev+1, "lev: " + str(lev), True)
            self.log.track(lev+1, "Std: " + str(std), True)
            self.log.track(lev+1, "Chg: " + str(chg), True)

        size = (std + chg) * 1.0
        norm = size / (self.cfg.max_msg_size * 1.0)
        if norm > 0.85:
            if self.cfg.tracking:
                self.log.track(lev+2, "Data Too Big for Uplink.", True)
                self.log.track(lev+3, "Data Norm: " + str(norm), True)
            return [ False, 4, "Data Too Big for Uplink." ], norm
        
        return [True, None, None ], norm

    #---------------------------------------------------------------
    # computeNumerator() class module.
    #---------------------------------------------------------------
    def computeNumerator(self, lev, sco, qua, siz, trg ):
        if self.cfg.tracking:
            self.log.track(lev, "Entering 'computeNumerator() Module.", True)
            self.log.track(lev+1, "lev: " + str(lev), True)
            self.log.track(lev+1, "sco: " + str(sco), True)
            self.log.track(lev+1, "qua: " + str(qua), True)
            self.log.track(lev+1, "siz: " + str(siz), True)
            self.log.track(lev+1, "trg: " + str(trg), True)

        try:
            scor = (self.cfg.pipo_scor_wt**2) * sco
            qual = (self.cfg.pipo_qual_wt**2) * qua
            size = (self.cfg.pipo_size_wt**2) * siz
            trig = (self.cfg.pipo_trig_wt**2) * trg
            numerator = (scor + qual + size + trig) * 1.0
        except Exception as e:
            return [ False, "P110", "PIPO Numerator Calculation Error: " + str(e)], 0.0

        if self.cfg.tracking:
            self.log.track(lev+1, "scor comp: " + str(scor), True)
            self.log.track(lev+1, "qual comp: " + str(qual), True)
            self.log.track(lev+1, "size comp: " + str(size), True)
            self.log.track(lev+1, "trig comp: " + str(trig), True)
            self.log.track(lev+1, "numerator: " + str(numerator), True)

        return [True, None, None ], numerator

    #---------------------------------------------------------------
    # computeDenominator() class module.
    #---------------------------------------------------------------
    def computeDenominator(self, lev, metafile):
        if self.cfg.tracking:
            self.log.track(lev, "Entering 'computeDenominator() Module.", True)
            self.log.track(lev+1, "lev: " + str(lev), True)
            self.log.track(lev+1, "met: " + str(metafile), True)

        try:
            ts = time.time()
            now = int(math.floor(ts / 3600)) * 1.0
            st = os.stat(metafile)
            meta_age = int(math.floor(st.st_mtime / 3600)) * 1.0
            pipo_age = (now - meta_age) * 1.0
            denominator = (pipo_age * self.cfg.pipo_time_wt) + 1.0
        except Exception as e:
            return [ False, "P120", "PIPO Age Calculation Failed" + str(e) ], None

        if self.cfg.tracking:
            self.log.track(lev+1, "System Time:      " + str(ts), True)
            self.log.track(lev+1, "Now Age (hours):  " + str(now), True)
            self.log.track(lev+1, "Meta Age (hours): " + str(meta_age), True)
            self.log.track(lev+1, "PIPO Age (hours): " + str(pipo_age), True)
            self.log.track(lev+1, "PIPO Denominator: " + str(denominator), True)

        return [True, None, None ], denominator
