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
##  Revision:   1.5 2019/04/19  13:30:00
##  Comment:    Fix max sizes in size normalization() method.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
##  Revision:   1.5 2019/04/19  12:15:00
##  Comment:    Fix uint8 byte size in size calculation.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
##  Revision:   1.4 2019/04/15  09:45:00
##  Comment:    Upgrade to new Node Type'code' Converison Table.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
##  Revision:   1.3 2019/04/05  11:15:00
##  Comment:    Upgrade/Fixes for 5/1 Deliverable; Err Msg Handling.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
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
import ast
import math
import time 
from bothelp import readFloatFile

########################################################################
##  The Float PIPO Class
########################################################################

class BotPIPO(object):

    def __init__(self, _cfg, _log, _lev, _db):
        self.cfg = _cfg
        self.log = _log
        self.db = _db
        self.scor = self.cfg.pipo_scor_wt
        self.qual = self.cfg.pipo_qual_wt
        self.size = self.cfg.pipo_size_wt
        self.trig = self.cfg.pipo_trig_wt
        self.time = self.cfg.pipo_time_wt
        self.purg = self.cfg.purge_rating
        if self.cfg.tracking:
            self.log.track(_lev, 'Creating PIPO Class Object.', True)
            self.log.track(_lev+1, "scor = " + str(self.scor), True)
            self.log.track(_lev+1, "qual = " + str(self.qual), True)
            self.log.track(_lev+1, "size = " + str(self.size), True)
            self.log.track(_lev+1, "trig = " + str(self.trig), True)
            self.log.track(_lev+1, "time = " + str(self.time), True)
            self.log.track(_lev+1, "purg = " + str(self.purg), True)

    #---------------------------------------------------------------
    # getPIPO() class module.
    #---------------------------------------------------------------
    # Calculate the PIPO Rating for a Data Product.

    def getPIPO(self, lev, metafile, metajson, trigger):
        if self.cfg.tracking:
            self.log.track(lev, "Entering 'getpipo()' Module.", True)
            self.log.track(lev+1, "Log Level: " + str(lev), True)
            self.log.track(lev+1, "Meta File: " + str(metafile), True)
            self.log.track(lev+13, "Meta JSON: " + str(metajson), True)
            self.log.track(lev+1, "Trigger:   " + str(trigger), True)
            self.log.track(lev, "Compute Data Folder from Meta Path.", True)

        try:
            datafolder = os.path.dirname(metafile)
            if self.cfg.tracking:
                self.log.track(lev+1, "Folder: " + str(datafolder), True)
        except Exception as e:
            enum = "P001"
            emsg = "getPIPO(): [" + str(e) + "]"
            if self.cfg.tracking:
                self.log.errtrack(str(enum), str(emsg))
            return [ False, str(enum), str(emsg) ], None

        #-----------------------------------------------------------
        # Get Node Code and Index Info from 'Node' Table.
        #-----------------------------------------------------------
        
        if self.cfg.tracking:
            self.log.track(lev, "Get Node Type-Instance Record from 'node' Table.", True)

        try:
            node_type = str(metajson['node_type'])
            node_instance = str(metajson['instance'])
        except Exception as e:
            enum = "P121"
            emsg = "getPIPO(): [" + str(e) + "]"
            if self.cfg.tracking:
                self.log.errtrack(str(enum), str(emsg))
            return [ False, str(enum), str(emsg) ], None

        sql = "SELECT * FROM node WHERE node_type = '"+ str(node_type) + "' AND node_instance = '" + str(node_instance) + "' LIMIT 1"
        success, node_results = self.db.getResults(lev+1, sql, False)

        if not success[0] or not node_results:
            # This DP NOT in 'node' Table; initialize one.
            if self.cfg.tracking:
                self.log.track(lev+1, "NO 'node' Table Entry for Type-Instance.", True)
                self.log.track(lev+1, "See if Node Type is Valid.", True)

            sql = "SELECT * FROM node WHERE node_type = '"+ str(node_type) + "' AND node_instance = '0' LIMIT 1"
            success, node_results = self.db.getResults(lev+2, sql, False)

            if not success[0] or not node_results:
                enum = "P122"
                emsg = "getPIPO(): [ Invalid/Inaccessible Node Type: " + str(node_type) + "]"
                if self.cfg.tracking:
                    self.log.errtrack(str(enum), str(emsg))
                return [ False, str(enum), str(emsg) ], None

            if self.cfg.tracking:
                self.log.track(lev+1, "Node Type Valid.", True)
                self.log.track(lev+1, "Create 'node' Table Entry for Instance.", True)

            node_code = node_results[0][1]
            node_index = 0
            sql = "INSERT INTO node VALUES ('%s',%i,%i,%i)" % (
                str(node_type),
                int(node_code),
                int(node_instance),
                int(node_index)
            )
            success = self.db.update(lev+2, sql)
        else:
            if self.cfg.tracking:
                self.log.track(lev+1, "A 'node' Table Entry Exists.", True)
            node_code = node_results[0][1]
            node_index = node_results[0][3]

        if self.cfg.tracking:
            self.log.track(lev+2, "Node Type:     " + str(node_type), True)
            self.log.track(lev+2, "Node Code:     " + str(node_code), True)
            self.log.track(lev+2, "Node Instance: " + str(node_instance), True)
            self.log.track(lev+2, "Node Index:    " + str(node_index), True)

        #-----------------------------------------------------------
        # Evaluate 'Standard' Data Product.
        #-----------------------------------------------------------
        if self.cfg.tracking:
            self.log.track(lev, "Evaluate 'Standard' Data Product.", True)
            self.log.track(lev+1, "Get 'Standard' Name Keyword-Value Pair.", True)

        std_file_name = None
        std_file_path = None
        std_head_bytes = 16
        std_data_items = 0
        std_data_type = None
        std_data_bytes = 0
        std_msg_size = 0
        stdjson = None
        
        try:
            std_file_name = str(metajson['data_file'])
        except:
            enum = "P011"
            emsg = "getPipo(): Mandatory 'Standard' File Name Keyword Missing."
            if self.cfg.tracking:
                self.log.errtrack(str(enum), str(emsg))
            return [ False, str(enum), str(emsg) ], None

        if std_file_name == "":
            enum = "P012"
            emsg = "getPipo(): Mandatory 'Standard' File Name Value Missing."
            if self.cfg.tracking:
                self.log.errtrack(str(enum), str(emsg))
            return [ False, str(enum), str(emsg) ], None

        std_file_path = datafolder + "/" + std_file_name
        if self.cfg.tracking:
            self.log.track(lev+1, "Name: " + str(std_file_name), True)
            self.log.track(lev+1, "Path: " + str(std_file_path), True)

        if self.cfg.tracking:
            self.log.track(lev+1, "Retrieve JSON File.", True)
        success, stdjson = readFloatFile(self.cfg, self.log, lev+13, std_file_path, False, True)
        if not success[0]:
            enum = "P015"
            emsg = "getPipo(): Mandatory 'Standard' Data JSON File Not Acquired."
            if self.cfg.tracking:
                self.log.errtrack(str(enum), str(emsg))
            return [ False, str(enum), str(emsg) ], None

        try:
            std_data_type = str(stdjson['dtype'])
            std_data_list = ast.literal_eval(str(stdjson['data']))
            std_data_items = len(std_data_list)
        except Exception as e:
            enum = "P016"
            emsg = "getPipo(): [" + str(e) + "]"
            if self.cfg.tracking:
                self.log.errtrack(str(enum), str(emsg))
            return [ False, str(enum), str(emsg) ], None

        if std_data_type == "float64" or std_data_type == "int64":
            std_data_bytes = std_data_items * 8
        elif std_data_type == "float32" or std_data_type == "int32":
            std_data_bytes = std_data_items * 4
        elif std_data_type == "uint8":
            std_data_bytes = std_data_items * 1
        else:
            enum = "P017"
            emsg = "getPipo(): Unknown Data Type [" + str(std_data_type) + "] encountered."
            if self.cfg.tracking:
                self.log.errtrack(str(enum), str(emsg))
            return [ False, str(enum), str(emsg) ], None

        std_msg_size = std_head_bytes + std_data_bytes

        if self.cfg.tracking:
            self.log.track(lev+1, "Std Head Bytes: " + str(std_head_bytes), True)
            self.log.track(lev+1, "Std Data List:  " + str(std_data_list), True)
            self.log.track(lev+1, "Std Data Items: " + str(std_data_items), True)
            self.log.track(lev+1, "Std Data Type:  " + str(std_data_type), True)
            self.log.track(lev+1, "Std Data Bytes: " + str(std_data_bytes), True)
            self.log.track(lev+1, "Std MSG Size:   " + str(std_msg_size), True)

        #-----------------------------------------------------------
        # Evaluate 'Change' Data Product.
        #-----------------------------------------------------------
        if self.cfg.tracking:
            self.log.track(lev, "Compute 'Change' Data Product size.", True)
            self.log.track(lev+1, "Get 'Change' Name Keyword-Value Pair.", True)

        chg_eligible = False
        chg_file_name = None
        chg_file_path = None
        chg_head_bytes = 12
        chg_data_type = None
        chg_data_list = []
        chg_data_items = 0
        chg_data_bytes = 0
        chg_msg_size = 0
        chgjson = None
        
        try:
            chg_file_name = str(metajson['change_file'])
        except:
            if self.cfg.tracking:
                self.log.track(lev+1, "No Change File Keyword Found; Continue.", True)

        if chg_file_name:
            if self.cfg.tracking:
                self.log.track(lev+1, "Name: " + str(chg_file_name), True)
                
            if chg_file_name == "NC" or chg_file_name == "nc":
                if self.cfg.tracking:
                    self.log.track(lev+1, "Keyword 'NC' Specified; Continue.", True)
            elif chg_file_name == "NULL" or chg_file_name == "Null" or chg_file_name == "null":
                if self.cfg.tracking:
                    self.log.track(lev+1, "Keyword 'NULL' Specified; Continue.", True)
            else:
                chg_file_path = datafolder + "/" + chg_file_name
                if self.cfg.tracking:
                    self.log.track(lev+2, "Name: " + str(chg_file_name), True)
                    self.log.track(lev+2, "Path: " + str(chg_file_path), True)
                    self.log.track(lev+1, "Retrieve JSON File.", True)

                success, chgjson = readFloatFile(self.cfg, self.log, lev+2, chg_file_path, False, True)
                if not success[0]:
                    if self.cfg.tracking:
                        self.log.track(lev+1, "Ignore; Continue.", True)
                else:
                    try:
                        chg_data_type = str(chgjson['dtype'])
                        chg_data_list = ast.literal_eval(str(chgjson['data']))
                        chg_data_items = len(chg_data_list)
                        chg_eligible = True # Set now, anticipating OK data_bytes calc.
                        if chg_data_type == "float64" or chg_data_type == "int64":
                            chg_data_bytes = chg_data_items * 8
                        elif chg_data_type == "float32" or chg_data_type == "int32":
                            chg_data_bytes = chg_data_items * 4
                        elif chg_data_type == "uint8":
                            chg_data_bytes = chg_data_items * 1
                        else:
                            chg_eligible = False    # One last switch back on final error.
                            if self.cfg.tracking:
                                self.log.track(lev+1, "Unknown Data Type [" + str(chg_data_type) + "] encountered.", True)
                                self.log.track(lev+1, "Ignore; Continue.", True)
                    except Exception as e:
                        chg_eligible = False    # One last switch back on final error.
                        if self.cfg.tracking:
                            self.log.track(lev+1, "Ignore; Continue.", True)

        chg_msg_size = chg_head_bytes + chg_data_bytes

        if self.cfg.tracking:
            self.log.track(lev+1, "Chg ELIGIBLE:   " + str(chg_eligible), True)
            self.log.track(lev+1, "Chg Head Bytes: " + str(chg_head_bytes), True)
            self.log.track(lev+1, "Chg Data List:  " + str(chg_data_list), True)
            self.log.track(lev+1, "Chg Data Items: " + str(chg_data_items), True)
            self.log.track(lev+1, "Chg Data Type:  " + str(chg_data_type), True)
            self.log.track(lev+1, "Chg Data Bytes: " + str(chg_data_bytes), True)
            self.log.track(lev+1, "Chg MSG Size:   " + str(chg_msg_size), True)
    
        #-----------------------------------------------------------
        # Get Normalized Data Product Size.
        #-----------------------------------------------------------
        if self.cfg.tracking:
            self.log.track(lev, "Compute 'Normalized' Data Product Size Factor.", True)

        success, norm = self.computeNormalized(lev+1, std_msg_size, chg_msg_size, chg_eligible)

        # Calculation of the PIPO file size 'normalization' should
        # NEVER fail; it may, however, calculate that the normalized
        # size is too big for transmission, rendering it ineligible.
        # If that's the case, insure the ineligible status.
        if not success[0]:
            chg_eligible = True     # Force to 'Ineligible'
            if self.cfg.tracking:
                self.log.track(lev, "Data Product NOT ELIGIBLE for Uplink.", True)

        #-------------------------------------------------------
        # Get PIPO Numerator.
        #-------------------------------------------------------
        if self.cfg.tracking:
            self.log.track(lev, "Get PIPO Formula Numerator.", True)
        
        success, numerator = self.computeNumerator(lev+1, float(metajson["node_id_score"]), float(metajson["quality"]), norm, trigger)

        # Calculation of the PIPO Numerator should NEVER fail; it's
        # a simple multiplier and adder calculation.  If it does,
        # the method returns 0.0, but guarantee this anyway.
        if not success[0]:
            numerator = 0.0

        #-----------------------------------------------------------
        # Compute PIPO Denominator.
        if self.cfg.tracking:
            self.log.track(lev, "Get PIPO Formula Denominator.", True)

        success, denominator = self.computeDenominator(lev+1, metafile)

        # Calculation of the PIPO Denominator should NEVER fail; it's
        # a simple multiplier and adder calculation.  If it does,
        # the method returns 1.0, but guarantee this anyway.
        if not success[0]:
            denominator = 1.0

        #-----------------------------------------------------------
        # Compute PIPO Rating.
        #-----------------------------------------------------------
        if self.cfg.tracking:
            self.log.track(lev, "Computing PIPO Rating.", True)

        pipo_rating = numerator / denominator
        if self.cfg.tracking:
            self.log.track(lev+1, "PIPO Rating: " + str(pipo_rating), True)

        return [ True, None, None], [ numerator, std_msg_size, chg_msg_size, chg_eligible, norm, pipo_rating, std_file_path, chg_file_path, stdjson, chgjson, node_code ]


    #---------------------------------------------------------------
    # computeNormalized() class module.
    #---------------------------------------------------------------
    def computeNormalized(self, _lev, _std, _chg, _eli):
        if self.cfg.tracking:
            self.log.track(_lev, "Enter computeNormalized() Method.", True)
            self.log.track(_lev+1, "_lev: " + str(_lev), True)
            self.log.track(_lev+1, "_std: " + str(_std), True)
            self.log.track(_lev+1, "_chg: " + str(_chg), True)
            self.log.track(_lev+1, "_eli: " + str(_eli), True)

        if not _eli or (_std <= _chg): 
            seg_size = _std
        else:
            seg_size = _chg

        if seg_size > self.cfg.max_msg_size - 32:
            enum = "P121"
            emsg = "Data Segment Exceeds Max Available Space for Inclusion."
            if self.cfg.tracking:
                self.log.track(_lev+1, str(emsg), True)
            return [ False, str(enum), str(emsg) ], None

        f_norm = 1.0 - ((seg_size * 1.0) / ((self.cfg.max_msg_size - 32) * 1.0))
        
        if self.cfg.tracking:
            self.log.track(_lev+1, "Norm: " + str(f_norm), True)

        return [True, None, None ], f_norm

    #---------------------------------------------------------------
    # computeNumerator() class module.
    #---------------------------------------------------------------
    def computeNumerator(self, lev, sco, qua, siz, trg):
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
