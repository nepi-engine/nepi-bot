########################################################################
##
##  Module: botsend.py
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
##  Revision:   1.12 2019/05/09  12:30:00
##  Comment:    Add correct DB closure upon Subsystem Exit.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
##  Revision:   1.11 2019/05/09  11:55:00
##  Comment:    Fix State setting from 1 to 2 when Sent.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
##  Revision:   1.10 2019/04/16  13:45:00
##  Comment:    Status/Meta Selection Loop Enhancements.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
##  Revision:   1.9 2019/04/16  07:30:00
##  Comment:    Enhancements to Status Message State Mgmt.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
##  Revision:   1.8 2019/04/08  06:30:00
##  Comment:    Major mods for Status/DP file/record Mgmt.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
##  Revision:   1.7 2019/04/01  15:15:00
##  Comment:    Release 4.1; builds but not tested w/Iridium.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
##  Revision:   1.6 2019/03/11  15:15:00
##  Comment:    Add Status and Meta Record Selection for Messaging.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
##  Revision:   1.5 2019/03/06  13:30:00
##  Comment:    Begin housekeeping development; file clean-up.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
##  Revision:   1.4 2019/02/19  09:30:00
##  Comment:    Add Archive PIPO recalculation management.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
##  Revision:   1.3 2019/02/12  09:50:00
##  Comment:    Completed PIPO evaluation management.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
##  Revision:   1.2 2019/02/04  09:50:00
##  Comment:    Deliverable 3/1; upgrade to new class libraries.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
##  Revision:   1.1 2019/01/09  12:00:00
##  Comment:    Module Instantiation.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
########################################################################

import os
import sys
import time
import uuid
import json
import math
import shutil
import socket
import sqlite3
import botdefs
from array import array
from botcfg import BotCfg
from botlog import BotLog
from botdb import BotDB
from botmsg import BotMsg
from botpipo import BotPIPO
from botcomm import BotComm
from bothelp import getAllFolderNames, getAllFileNames
from bothelp import readFloatFile, triggerScoreLookup, resetCfgValue
from bothelp import deleteFolder, deleteDataProduct

########################################################################
# Instantiate a NEPI-Bot Configuration Class (from 'botcfg.py')
########################################################################

cfg = BotCfg()
cfg.initcfg()

########################################################################
##  Instantiate Bot-Send Debug/Log Object (from 'botlog.py')
########################################################################

log = BotLog(cfg, "BOT-SEND")
log.initlog(0)

########################################################################
# Instantiate a NEPI-Bot Database Class (from 'botdb.py').
########################################################################

db = BotDB(cfg, log, 0)
success, dbconn = db.getconn(0)

if not success[0]:
    sys.exit(1)

########################################################################
# Instantiate a NEPI-Bot PIPO Class (from 'botpipo.py').
########################################################################

pipo = BotPIPO(cfg, log, 0, db)

########################################################################
# Instantiate a NEPI-Bot Message Class (from 'botmsg.py').
########################################################################

sm = BotMsg(cfg, log, 1)

########################################################################
# Re-Evaluate PIPO Ratings for Archived Data Products if Required.
########################################################################

if cfg.tracking:
    log.track(0, "Recalculate Archived PIPO Ratings.", True)
    log.track(1, "Select 'Active' Records from Database.", True)

sql = "SELECT rowid, numerator, trigger, quality, score, metafile, norm FROM meta WHERE state = '0'"
success, rows = db.getResults(2, sql, False)

if success[0]:
    if rows == []:
        log.track(1, "No Active Records in Database; Continue.", True)
    else:
        log.track(1, "Active Records in Database.", True)

        if cfg.wt_changed:
            if cfg.tracking:
                log.track(1, "Weight Factors Have Changed.", True)
        else:
            if cfg.tracking:
                log.track(1, "Weight Factors Are Unchanged.", True)

        for row in rows:
            rwid = int(row[0])
            numr = float(row[1])
            trig = float(row[2])
            qual = float(row[3])
            scor = float(row[4])
            meta = str(row[5])
            norm = float(row[6])
            if cfg.tracking:
                log.track(1, "Reevaluating Data Product ID: " + str(rwid), True)
                log.track(2, "Numr: " + str(numr), True)
                log.track(2, "Trig: " + str(trig), True)
                log.track(2, "Qual: " + str(qual), True)
                log.track(2, "Scor: " + str(scor), True)
                log.track(2, "Norm: " + str(norm), True)
                log.track(2, "Meta: " + str(meta), True)

            if cfg.wt_changed:
                if cfg.tracking:
                    log.track(2, "Recalculate Numerator.", True)
                success, numerator = pipo.computeNumerator(3, scor, qual, norm, trig)
            else:
                if cfg.tracking:
                    log.track(2, "Use existing Numerator." + str(numr), True)
                numerator = numr

            if cfg.tracking:
                log.track(2, "Recalculate Denominator.", True)
            success, denominator = pipo.computeDenominator(3, meta)

            if cfg.tracking:
                log.track(2, "Recalculcate PIPO Rating: ", False)
            pipo_rating = numerator / denominator
            if cfg.tracking:
                log.track(2, str(pipo_rating), True)

            sql = "UPDATE meta SET numerator=" + str(numerator) + ", pipo=" + str(pipo_rating) + " WHERE rowid=" + str(rwid)

            success = db.update(2, sql)

        if cfg.wt_changed:
            if cfg.tracking:
                log.track(1, "All Archive PIPOs Recalculated; Reset Config JSON.", True)

            resetCfgValue(cfg, log, 2, "wt_changed", 0)

########################################################################
# Retrieve a list of the Float's Data Folders; Sort and Reverse.
########################################################################
#
# If attempt to get Data Product folders fails, this may be a temporary
# condition; just carry on because older Data Products may still be
# eligible for an uplink.

if cfg.tracking:
    log.track(0, "Get ALL Data Folders in Data Dir.", True)

success, allfolders = getAllFolderNames(cfg, log, 1, cfg.data_dir_path, True, True)

########################################################################
# Retrieve New (All) Status and Data Product Files; Populate the DB.
########################################################################

haveNewStatus = 0

for dir in allfolders:
    #-------------------------------------------------------------------
    # Deal With Status Record First.
    #-------------------------------------------------------------------
    # Get the ONLY sys_status.json file that should be in THIS Data
    # Product folder. Deconstruct it into a JSON object and load into
    # the 'status' table of the embedded DB.  If any failure detected,
    # DESTROY the entire DP Folder since, without this Status Record,
    # all the Data Products in this DP Folder are useless.

    data_prod_folder = cfg.data_dir_path + "/" + str(dir)
    status_file_path = data_prod_folder + "/" + cfg.sys_status_file
    if cfg.tracking:
        log.track(0, "", True)
        log.track(0, "Processing Status File.", True)
        log.track(1, "Data Folder: " + str(data_prod_folder), True)
        log.track(1, "Status File: " + str(cfg.sys_status_file), True)
        log.track(1, "Consume File from DP Directory.", True)

    success, status_json = readFloatFile(cfg, log, 2, status_file_path, False, True)

    if success[0]:
        if cfg.tracking:
            log.track(1, "INSERT Status Record into DB 'status' Table.", True)

        # Insert Status Record and capture the DB 'rowid' (which will be
        # the DB foreign key - or, pointer - back to this record in the
        # DB, that is, embedded in all subsequent Data Product records
        # associated with this Status record.
        success, status_rowid = db.pushstat(2, status_json)
        if not success[0]:
            if cfg.tracking:
                log.track(2, "Status Record Insertion Failed.", True)
                log.track(2, "Entire DP Folder Unusable; Delete it.", True)

            deleteFolder(cfg, log, 3, data_prod_folder)
            if cfg.tracking:
                log.track(2, "Continue.", True)
            continue

    else:
        if cfg.tracking:
            log.track(2, "Status File NOT Acquired.", True)
            log.track(2, "Consider Entire DP Folder Corrupt; DESTROY.", True)

        deleteFolder(cfg, log, 3, data_prod_folder)
        if cfg.tracking:
            log.track(2, "Continue.", True)
        continue

    # Retain this 'latest' Status Record; these will be retrieved later
    # in order to have at least one placed first in the uplink message.
    if haveNewStatus == 0:
        haveNewStatus = status_rowid
        if cfg.tracking:
            log.track(2, "Have at Least 1 New Status Record for Uplink Msg.", True)
            log.track(3, "ID: " + str(haveNewStatus), True)

    #-------------------------------------------------------------------
    # Calculate Trigger Score
    #-------------------------------------------------------------------
    if cfg.tracking:
        log.track(1, "Calculate Trigger Score.", True)

    trigger, wet, wei = triggerScoreLookup(cfg, log, 2, status_json)
    if cfg.tracking:
        log.track(2, "Trigger Score:   " + str(trigger), True)
        log.track(2, "Wake Event Type: " + str(wet), True)
        log.track(2, "Wake Event ID: " + str(wei), True)

    #-------------------------------------------------------------------
    # Find All META Data Files
    #-------------------------------------------------------------------
    # The 'sys_status.json' file for this Data Folder is now in the DB;
    # we need to find ALL Meta Data files (*_meta.json) in this same
    # Folder (the Status File we're processing right now applies to all
    # Meta Files in this same Data Folder).
    if cfg.tracking:
        log.track(1, "Get ALL DP Files in This Data Folder.", True)
        log.track(2, "Folder:" + data_prod_folder, True)

    success, allfiles = getAllFileNames(cfg, log, 2, data_prod_folder, False, False)
    if not success[0] or not allfiles:
        if cfg.tracking:
            log.track(2, "Data Folder File NOT ACCESSIBLE.", True)
            log.track(2, "Remove Entire DP Folder.", True)

        deleteFolder(cfg, log, 3, data_prod_folder)
        if cfg.tracking:
            log.track(2, "Continue.", True)
        continue

    # Retrieve all "meta" files out of the list.
    allmetafiles = []
    for f in allfiles:
        if "_meta.json" in f:
            allmetafiles.append(f)

    if not allmetafiles:
        if cfg.tracking:
            log.track(2, "NO 'Meta' DP Files in this Data Folder.", True)
            log.track(2, "Done Processing; Remove Entire DP Folder.", True)

        deleteFolder(cfg, log, 3, data_prod_folder)
        if cfg.tracking:
            log.track(2, "Continue.", True)
        continue
            #log.track(2, "Update Status File with 'meta_state' of 1.", True)

        #sql = "UPDATE status SET meta_state=1 WHERE rowid=" + str(status_rowid)
        #success = db.update(3, sql)
        #if cfg.tracking:
            #log.track(2, "Continue.", True)
        #continue

    if cfg.tracking:
        log.track(2, "Got All 'Meta' DP Files.", True)
        log.track(3, "Files: " + str(allmetafiles), True)

    #-------------------------------------------------------------------
    # Process Each META File
    #-------------------------------------------------------------------
    for mf in allmetafiles:
        # First, compose the full path to the Meta File.
        meta_file_path = data_prod_folder + "/" + mf
        if cfg.tracking:
            log.track(1, "Processing META File: " + str(mf), True)
            log.track(2, "Path: " + str(data_prod_folder), True)
            log.track(2, "Consume File from DP Directory.", True)

        # Now process each Meta File by calculating its PIPO Rating,
        # which evaluates the mandatory 'Standard' Data File and any
        # optional 'Change' Data File.
        success, meta_json = readFloatFile(cfg, log, 3, meta_file_path, False, True)
        if not success[0]:
            if cfg.tracking:
                log.track(2, "Can't Access the Data Product's 'Meta' File.", True)
                log.track(2, "These DP Files are Useless; Remove Them.", True)

            deleteDataProduct(cfg, log, 3, meta_file_path)
            if cfg.tracking:
                log.track(2, "Move on to next DP; Continue.", True)
            continue

        #---------------------------------------------------------------
        # Compute the PIPO Rating
        #---------------------------------------------------------------
        # For each Meta File, get the PIPO rating (which also verifies
        # and returns the mandatory "Standard" and optional "Change"
        # Data Files that are identified in the Meta File).

        if cfg.tracking:
            log.track(2, "Get the PIPO Rating.", True)

        success, info = pipo.getPIPO(3, meta_file_path, meta_json, trigger)
        if not success[0]:
            if cfg.tracking:
                log.track(2, "Can't Compute PIPO rating.", True)
                log.track(2, "This DP is of No value; Remove.", True)

            deleteDataProduct(cfg, log, 3, meta_file_path)
            if cfg.tracking:
                log.track(2, "Move on to next DP; Continue.", True)
            continue

        #---------------------------------------------------------------
        # INSERT the META Data Record into the Float's Database.
        #---------------------------------------------------------------
        if cfg.tracking:
            log.track(2, "INSERT Meta Data Record into DB.", True)

        #success = db.pushmeta(3, meta_json, info[3], status_rowid, trigger, info[0], info[1], info[2], info[4], info[5], meta_file_path, stdfile, chgfile)
        success = db.pushmeta(3, meta_json, info, status_rowid, trigger, meta_file_path)

        if not success[0]:
            if cfg.tracking:
                log.track(2, "Can't Insert Meta Record into DB." + str(mf), True)
                log.track(2, "This DP is of No value; Remove." + str(mf), True)

            deleteDataProduct(cfg, log, 3, meta_file_path)
            if cfg.tracking:
                log.track(2, "Move on to next DP; Continue.", True)
            continue

        #---------------------------------------------------------------
        # DONE LOOP-PROCESSING NEW SDK DATA PRODUCTS.
        #---------------------------------------------------------------

########################################################################
# Housekeeping of Data Directory.
########################################################################
# In theory, all the Data Products should have been processed if we get
# to this point.  I case of errors, those offending Data Products have
# already been removed.  Now, we should clean up any residue in the
# entire Data Directory.

if cfg.tracking:
    log.track(0, "Clean Up the Entire Data Directory.", True)
    log.track(1, "Get Remaining Data Folders in Data Dir.", True)

success, allfolders = getAllFolderNames(cfg, log, 2, cfg.data_dir_path, True, True)

if not success[0]:
    if cfg.tracking:
        log.track(1, "UNABLE to perform Clean-Up.", True)
elif (allfolders == None) or (allfolders == []):
    if cfg.tracking:
        log.track(1, "Nothing remaining to Clean-Up.", True)
else:
    for folder in allfolders:
        dir = cfg.data_dir_path + "/" + str(dir)
        deleteFolder(cfg, log, 1, dir)

if cfg.tracking:
    log.track(1, "Clean-Up Completed; Continue.", True)

########################################################################
# Create the Float Message.
########################################################################

if cfg.tracking:
    log.track(0, "Create the Float Message.", True)

########################################################################
# Add at Least One Status Segment If NEWEST in DB Not Yet Sent.
########################################################################
# If there is a 'latest' Status Message from the current wake-up search,
# pack the first one in the Result Set (that should be the latest Status
# Record which should be sent whether it has associated Data Products or
# not). Note that it is ordered by 'timestamp' to percolate the latest
# Status Record to the top of the list. If a new Status Record is found,
# it will be placed first in the uplink Message. Otherwise, skip this
# process and continue on processing both new or previoisly-stored Data
# Products.

if not haveNewStatus:
    if cfg.tracking:
        log.track(1, "No NEW Status Messages Processed; Skip This Step.", True)
else:
    if cfg.tracking:
        log.track(1, "Find 'Latest' Active Status Record for Bot Message.", True)

    sql = "SELECT rowid, * FROM status WHERE state = '0' ORDER BY timestamp DESC LIMIT 1"
    success, new_stat_results = db.getResults(2, sql, False)

    if success[0]:
        if new_stat_results == None:
            if cfg.tracking:
                log.track(2, "Can't Find 'Latest' Active Status Record in DB.", True)
                log.track(3, "Ignore; Continue.", True)
        else:
            if cfg.tracking:
                log.track(2, "Found 'Latest' Active Status Record in Database.", True)
                log.track(3, "rowid=[" + str(new_stat_results[0][0]) + "]", True)
                log.track(3, "state=[" + str(new_stat_results[0][1]) + "]", True)
                log.track(3, "timestamp=[" + str(float(new_stat_results[0][6])) + "]", True)
                log.track(2, "Add This 'Latest' Status Record to Uplink Message.", True)

            success = sm.packstat(3, new_stat_results[0])
            if success[0]:
                if cfg.tracking:
                    log.track(1, "Latest Status Record Successfully Packed.", True)
                    log.track(1, "Update Status Record 'state' to 'packed.'", True)

                    sql = "UPDATE status SET state = '1' WHERE rowid = '" + str(new_stat_results[0][0]) + "'"
                    success = db.update(2, sql)
                    if not success[0]:
                        if cfg.tracking:
                            log.track(1, "Well ... This is Awkward.", True)
                            log.track(1, "Probably Come Back to Bite Us in the Ass.'", True)
                            log.track(1, "May Eventually Repack This Same Status Record.", True)
            else:
                if cfg.tracking:
                    log.track(2, "Can't Pack 'Latest' Active Status Record.", True)
                    log.track(2, "Ignore; Continue.", True)
    else:
        if cfg.tracking:
            log.track(2, "ERROR Accessing 'Latest' Active Status Record from DB.", True)
            log.track(2, "Ignore; Continue with Data Products.", True)


########################################################################
# Select Active/Unsent Meta Records for the Float Message.
########################################################################

if cfg.tracking:
    log.track(0, "Select Top Active Data Products for Uplink Message.", True)

have_active_dp = False

sql = "SELECT rowid, * FROM meta WHERE state = '0' ORDER BY pipo DESC LIMIT 32"
success, meta_rows = db.getResults(1, sql, False)

if success[0]:
    if meta_rows == None:
        if cfg.tracking:
            log.track(1, "NO Active Data Products in Database; Continue.", True)
    else:
        have_active_dp = True
        if cfg.tracking:
            log.track(1, "Active Data Products FOUND in Database.", True)
            log.track(2, "Total: " + str(len(meta_rows)), True)
            for row in meta_rows:
                log.track(14, "rowid=[" + str(row[0]) + "]  statid=[" + str(row[3]) + "]  pipo=[" + str(row[6]) + "]", True)
else:
    if cfg.tracking:
        log.track(1, "ERROR Finding Active Data Products in DB; Continue.", True)


########################################################################
# Fill Balance of Uplink Message with Top Active Data Products.
########################################################################
# Cycle through all active/unsent Data Products and add to the Float
# message if they fit.

if meta_rows:
    for row in meta_rows:
        metaID = str(row[0])
        statusFK = row[3]
        stat_state = 0
        stat_timestamp = 0
        if cfg.tracking:
            log.track(1, "Process Meta Data Product ID #" + str(metaID), True)
            log.track(2, "Get State/Timestamp of Associated Status Record.", True)
            log.track(3, "Status FK: " + str(statusFK), True)

        # If not already packed in this Message, get it out of the 'status'
        # Table in DB. Then, pack this Status Record in the uplink Message
        # and save the Status Record's timestamp.
        #if not stat_sent_flag:
        sql = "SELECT rowid, * FROM status WHERE rowid = '" + str(statusFK) + "'"
        success, assoc_statrec = db.getResults(3, sql, False)
        if success[0]:
            if not assoc_statrec:
                if cfg.tracking:
                    log.track(3, "Can't Access Assoc Status Record in DB.", True)
                    log.track(3, "Likely a Stale Record; Ignore this DP; Continue.", True)
                continue
            else:
                stat_state = assoc_statrec[0][1]
                stat_timestamp = assoc_statrec[0][6]

                if stat_state == 1:
                    if cfg.tracking:
                        log.track(3, "Assoc Status Record JUST PACKED into This Msg.", True)
                        log.track(4, "State:     " + str(stat_state), True)
                        log.track(4, "Timestamp: " + str(stat_timestamp), True)
                elif assoc_statrec[0][1] > 0:
                    if cfg.tracking:
                        log.track(3, "Assoc Status Record Previously Sent per DB.", True)
                        log.track(4, "State:     " + str(stat_state), True)
                        log.track(4, "Timestamp: " + str(stat_timestamp), True)
                else:
                    if cfg.tracking:
                        log.track(3, "Assoc Status Record FOUND in DB, but NOT Sent.", True)
                        log.track(3, "PACK it into THIS Uplink Message.", True)
                        log.track(4, "State:     " + str(stat_state), True)
                        log.track(4, "Timestamp: " + str(stat_timestamp), True)
                    success = sm.packstat(4, assoc_statrec[0])
                    if success[0]:
                        if cfg.tracking:
                            log.track(3, "PACKED FK Status Record into Message.", True)
                    else:
                        if cfg.tracking:
                            log.track(3, "Can't PACK FK Status Record into Message.", True)
                            log.track(3, "IGNORE this DP; Continue. ", True)
                        continue
        else:
            if cfg.tracking:
                log.track(3, "Status Record NOT Available from DB.", True)
                log.track(3, "Ignore This DP; Continue.", True)
            continue

        #---------------------------------------------------------------
        # Get the Next 'index' value from 'node' Table in DB.
        #---------------------------------------------------------------
        ntype = str(row[7])
        ninst = str(row[8])
        if cfg.tracking:
            log.track(2, "Get the Next 'index' value from 'node' Table in DB", True)

        sql = "SELECT rowid, * FROM node WHERE node_type = '"+ str(ntype) + "' AND node_instance = '" + str(ninst) + "' LIMIT 1"
        success, node_results = db.getResults(3, sql, False)

        if not success[0] or not node_results:
            if cfg.tracking:
                log.track(3, "Inaccessible Node Type/Instance: " + str(ntype) + "/" + str(ninst), True)
                log.track(3, "Ignore This DP; Continue.", True)
            continue

        if not node_results:
            if cfg.tracking:
                log.track(3, "Node Type/Instance NOT Found: " + str(ntype) + "/" + str(ninst), True)
                log.track(3, "Ignore This DP; Continue.", True)
            continue

        node_index = int(node_results[0][4])
        node_stage = int(node_results[0][5])
        next_index = node_stage + 1
        if cfg.tracking:
            log.track(3, "Got Node Type/Instance: "+ str(ntype) + "/" + str(ninst), True)
            log.track(4, "Index: " + str(node_index), True)
            log.track(4, "Stage: " + str(node_index), True)
            log.track(4, "Next:  " + str(next_index), True)

        #---------------------------------------------------------------
        # Got Associated Status Record, so PACK this Data Product.
        #---------------------------------------------------------------
        if cfg.tracking:
            log.track(2, "Pack This Data Product into Uplink Message.", True)

        success = sm.packmeta(3, row, stat_timestamp, next_index)
        if success[0]:
            if cfg.tracking:
                log.track(3, "PACKED Data Product Record into Message.", True)
                log.track(3, "Update Meta Record 'state' to 'packed.'", True)

                sql = "UPDATE meta SET state = '1' WHERE rowid = '" + str(row[0]) + "'"
                success = db.update(4, sql)
                if not success[0]:
                    if cfg.tracking:
                        log.track(4, "Well ... This is Awkward.", True)
                        log.track(4, "Probably Come Back to Bite Us in the Ass.'", True)
                        log.track(4, "May Eventually Repack This Same Meta Record.", True)
        else:
            if cfg.tracking:
                log.track(3, "Can't PACK Data Product Record.", True)
                continue

        #---------------------------------------------------------------
        # Data Product Packed; Update 'node' Table in DB.
        #---------------------------------------------------------------

        if cfg.tracking:
            log.track(1, "Update 'node' Table with latest Index.'", True)

            sql = "UPDATE node SET node_stage = '" + str(next_index) + "' WHERE rowid = '" + str(node_results[0][0]) + "'"
            success = db.update(2, sql)
            if not success[0]:
                if cfg.tracking:
                    log.track(2, "Well ... This is Awkward.", True)
                    log.track(2, "Probably Come Back to Bite Us in the Ass.'", True)
                    log.track(2, "Will wind up reverting back to 'std' DP Delivery.", True)

        

if cfg.tracking:
    log.track(1, "Final Message Complete.", True)
    log.track(2, "Buf Len: " + str(len(str(sm.buf))), True)
    log.track(2, "Buf Msg: " + str(sm.buf).encode("hex"), True)

########################################################################
# Open Communications Port and Send the Message Buffer.
########################################################################

if cfg.tracking:
    log.track(0, "Send the Message Buffer.", True)

if sm.len > 0:
    bc = BotComm(cfg, log, cfg.type, 1)
    success = bc.getconn(1)
    if success[0]:
        send_success, cnc_msgs = bc.send(1, sm.buf, 5)
    else:
        send_success = [ False, None, None ], None
    success = bc.close(1)
else:
    if cfg.tracking:
        log.track(1, "NO Message to Send.", True)

########################################################################
# Database Housekeeping.
########################################################################

if cfg.tracking:
    log.track(0, "Peform DB Housekeeping.", True)

if sm.len > 0:
    if send_success[0]:
        if cfg.tracking:
            log.track(1, "Update Bit-Packed Status Record(s) to 'sent' Status.", True)

        sql = "UPDATE status SET state = '2' WHERE state = '1'"
        success = db.update(2, sql)
        if not success[0]:
            if cfg.tracking:
                log.track(2, "Well ... This is Awkward.", True)
        else:
            if cfg.tracking:
                log.track(2, "Done.", True)

        if cfg.tracking:
            log.track(1, "Update Bit-Packed Meta Record(s) to 'sent' Status.", True)

        sql = "UPDATE meta SET state = '2' WHERE state = '1'"
        success = db.update(2, sql)
        if not success[0]:
            if cfg.tracking:
                log.track(2, "Well ... This is Awkward.", True)
        else:
            if cfg.tracking:
                log.track(2, "Done.", True)

        if cfg.tracking:
            log.track(1, "Update 'node' Table.", True)

        sql = "UPDATE node SET node_index = node_stage"
        success = db.update(2, sql)

        if not success[0]:
            if cfg.tracking:
                log.track(2, "Well ... This is Awkward.", True)
        else:
            if cfg.tracking:
                log.track(2, "Done.", True)

    else:
        if cfg.tracking:
            log.track(1, "Reset Bit-Packed Status Record(s) to 'new/active' Status.", True)

        sql = "UPDATE status SET state = '0' WHERE state = '1'"
        success = db.update(2, sql)
        if not success[0]:
            if cfg.tracking:
                log.track(2, "Well ... This is Awkward.", True)
        else:
            if cfg.tracking:
                log.track(2, "Done.", True)

        if cfg.tracking:
            log.track(1, "Reset Bit-Packed Meta Record(s) to 'new/active' Status.", True)

        sql = "UPDATE meta SET state = '0' WHERE state = '1'"
        success = db.update(2, sql)
        if not success[0]:
            if cfg.tracking:
                log.track(2, "Well ... This is Awkward.", True)
        else:
            if cfg.tracking:
                log.track(2, "Done.", True)

        if cfg.tracking:
            log.track(1, "Update 'node' Table.", True)

        sql = "UPDATE node SET node_stage = node_index"
        success = db.update(2, sql)

        if not success[0]:
            if cfg.tracking:
                log.track(2, "Well ... This is Awkward.", True)
        else:
            if cfg.tracking:
                log.track(2, "Done.", True)
else:
    if cfg.tracking:
        log.track(1, "NO Housekeeping to be Done.", True)
    
success = db.close(0)

########################################################################
# Close the Bot-Send Subsystem.
########################################################################

if cfg.tracking:
    log.track(0, "", True)
    log.track(0, "Bot-Send Subsystem Closing.", True)
    log.track(0, "", True)

sys.exit(0)

