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
##  Revision:   1.6 2019/04/01  15:15:00
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

########################################################################
# Instantiate a NEPI-Bot Configuration Class (from 'botcfg.py')
########################################################################

cfg = BotCfg()
cfg.initcfg()

########################################################################
##  Instantiate Bot-Send Debug/Log Object (from 'botlog.py')
########################################################################

log = BotLog(cfg, 0, "BOT-SEND")
log.initlog()

########################################################################
# Instantiate a NEPI-Bot Database Class (from 'botdb.py').
########################################################################

db = BotDB(cfg, log)
success, conn = db.getconn()

if not success:
    if cfg.tracking:
        log.errtrack(9999, 'BotDB CLASS FATAL SYSTEM EXIT.')
    sys.exit(1)

########################################################################
# Instantiate a NEPI-Bot PIPO Class (from 'botpipo.py').
########################################################################

pipo = BotPIPO(cfg, log)

########################################################################
# Re-Evaluate PIPO Ratings for Archived Data Products if Required.
########################################################################
if cfg.tracking:
    log.track(0, "Recalculate Archived PIPO Ratings.", True)
    log.track(1, "Select 'Active' Records from Database.", True)

sql = "SELECT rowid, numerator, trigger, quality, score, metafile, norm FROM meta WHERE state = '0'"
success, rows = db.getResults(2, sql, False)

if success[0]:
    if rows == None:
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
            rwid = row[0]
            numr = row[1]
            trig = row[2]
            qual = row[3]
            scor = row[4]
            meta = row[5]
            norm = row[6]
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

            sql = "UPDATE data SET numerator=" + str(numerator) + ", pipo=" + str(pipo_rating) + " WHERE rowid=" + str(rwid)

            success = db.update(2, sql)

        if cfg.wt_changed:
            if cfg.tracking:
                log.track(1, "All Archive PIPOs Recalculated; Reset Config JSON.", True)

            resetCfgValue(cfg, log, 2, "wt_changed", "0")

########################################################################
# Retrieve a list of the Float's Data Folders; Sort and Reverse.
########################################################################

if cfg.tracking:
    log.track(0, "Get ALL Data Folders in Data Dir.", True)

success, allfolders = getAllFolderNames(cfg, log, 1, cfg.data_dir_path, True, True)
if not success[0]:
    if cfg.tracking:
        log.errtrack(success[1], success[2])
    sys.exit(1)

########################################################################
# Retrieve New (All) Status and Data Product Files; Populate the DB.
########################################################################

if cfg.tracking:
    log.track(0, "Retrieve New Status and Data Product Files from SDK.", True)

haveLatestStatus = False

for dir in allfolders:
    # Get the ONLY sys_status.json file that should be in this Data
    # Product folder. Deconstruct it into a JSON object.
    data_prod_folder = cfg.data_dir_path + "/" + str(dir)
    status_file = data_prod_folder + "/" + cfg.sys_status_file
    if cfg.tracking:
        log.track(1, "Processing Status File", True)
        log.track(2, "Data Folder: " + str(data_prod_folder), True)
        log.track(2, "Status File: " + str(cfg.sys_status_file), True)

    success, status_json = readFloatFile(status_file, False, True)
    if cfg.tracking:
        log.track(2, "Acquired: " + str(success[0]), True)
        log.track(7, "Contents: " + str(status_json), True)

    # If no 'status' JSON file, DP Folder is Corrupt; Delete entire folder.
    if not success[0]:
        if cfg.tracking:
            log.track(1, "Status Record Not Acquired' DP Folder is Corrupt", True)
            log.track(2, "Err Num: " + str(success[1]), True)
            log.track(2, "Err Msg: " + str(success[2]), True)
            log.track(1, "Delete DP Folder: " + str(data_prod_folder), True)

        try:
            shutil.rmtree(data_prod_folder)
            log.track(2, "Success: Data Product Folder Removed.", True)
        except Exception as e:
            log.track(2, "ERROR: [" + str(e) + "]", True)

        continue

    #-------------------------------------------------------------------
    # Insert Status Record and capture the DB 'rowid' (which is the DB
    # foreign key - or, pointer - back to this record in the DB which
    # will be embedded in all future Data Product records accociated
    # with this Status record.
    if cfg.tracking:
        log.track(2, "Insert Status Record into DB.", True)

    success, status_rowid = db.pushstat(3, status_json)
    if cfg.tracking:
        log.track(3, "Row ID: " + str(status_rowid), True)

    if not success[0]:
        if cfg.tracking:
            log.errtrack(success[1], success[2])
        continue

    haveLatestStatus = True

    #-------------------------------------------------------------------
    if cfg.tracking:
        log.track(2, "Calculate Trigger Score.", True)

    trigger, wet, wei = triggerScoreLookup(status_json)
    if cfg.tracking:
        log.track(3, "Trigger Score:   " + str(trigger), True)
        log.track(3, "Wake Event Type: " + str(wet), True)
        log.track(3, "Wake Event ID: " + str(wei), True)

    #-------------------------------------------------------------------
    # The 'sys_status.json' file for this Data Folder is in the DB; we
    # need to find ALL Meta Data files (*_meta.json) in this same Folder
    # (the Status File we're processing right now applies to all the
    # Meta Files in this same Data Folder).
    if cfg.tracking:
        log.track(2, "Get ALL Meta Data Files in: " + data_prod_folder, True)

    success, allfiles = getAllFileNames(cfg, log, 3, data_prod_folder, False, False)
    if not success:
        if cfg.tracking:
            log.track(3, "NO Files in this Data Folder at all; Continue.", True)
        continue

    allmetafiles = []
    for f in allfiles:
        if "_meta.json" in f:
            allmetafiles.append(f)

    if not allmetafiles:
        if cfg.tracking:
            log.track(3, "NO Meta Data Files in this Data Folder; Continue.", True)

        sql = "UPDATE status SET meta_state=1 WHERE rowid=" + str(status_rowid)
        success = db.update(3, sql)
        continue

    if cfg.tracking:
        log.track(3, "Got All META Data Files: " + str(allmetafiles), True)

    #-------------------------------------------------------------------
    # Now process each Meta File by calculating its PIPO Rating and
    # investigating the mandatory 'Standard' Data File and any optional
    # 'Change' Data File.
    if cfg.tracking:
        log.track(2, "Process ALL Meta Files in.", True)

    for mf in allmetafiles:
        meta_file = data_prod_folder + "/" + mf
        if cfg.tracking:
            log.track(3, "Processing Meta File: " + str(meta_file), True)

        acquired, meta_json = readFloatFile(meta_file, False, True)
        if not acquired:
            if cfg.tracking:
                log.track(4, "META_JSON RECORD NOT ACQUIRED; Continue.", True)
            continue

        if cfg.tracking:
            log.track(4, "Acquired: " + str(acquired), True)
            log.track(9, "Contents: " + str(meta_json), True)

        #---------------------------------------------------------------
        # For each Meta File, get the PIPO rating (which also verifies
        # and returns the mandatory "Standard" and optional "Change"
        # Data Files that are identified in the Meta File).
        if cfg.tracking:
            log.track(4, "Get the PIPO Rating.", True)

        success, info = pipo.getPipo(5, meta_file, meta_json, trigger)
        if not success[0]:
            if cfg.tracking:
                log.track(4, "CAN'T PROCESS PIPO Rating; Continue.", True)
                continue

        stdfile = info[6]
        chgfile = info[7]
        #---------------------------------------------------------------
        # Put this Meta Data Product into the Float's Database.
        if cfg.tracking:
            log.track(4, "Put Meta Data Record into DB; Continue.", True)

        success = db.pushmeta(5, meta_json, info[3], status_rowid, trigger, info[0], info[1], info[2], info[4], info[5], meta_file, stdfile, chgfile)


########################################################################
# Select 'Latest' Active/Unsent Status Records for the Float Message.
########################################################################
if haveLatestStatus:
    if cfg.tracking:
        log.track(0, "Expecting 'Latest' Active Status Record for the Message.", True)

    sql = "SELECT rowid, * FROM status WHERE state = '0' ORDER BY timestamp DESC LIMIT 1"
    success, stat_latest_results = db.getResults(1, sql, False)

    if success[0]:
        if stat_latest_results == None:
            if cfg.tracking:
                log.track(1, "Can't find 'Latest' Active Status Record in DB.", True)
        else:
            if cfg.tracking:
                log.track(1, "Found 'Latest' Active Status Record in Database.", True)
                log.track(8, "statusID=[" + str(stat_latest_results[0][0]) + "]  timestamp=[" + str(float(stat_latest_results[0][6])) + "]", True)
else:
    if cfg.tracking:
        log.track(0, "NO 'Latest' Active Status Record for the Message.", True)

########################################################################
# Select Active/Unsent Meta Records for the Float Message.
########################################################################
if cfg.tracking:
    log.track(0, "Select Top Active Data Products for the Message.", True)

sql = "SELECT rowid, * FROM meta WHERE state = '0' ORDER BY pipo DESC LIMIT 16"
success, meta_rows = db.getResults(1, sql, False)

if success[0]:
    if meta_rows == None:
        if cfg.tracking:
            log.track(1, "No Active Data Products in Database; Continue.", True)
    else:
        if cfg.tracking:
            log.track(1, "Active Data Products in Database.", True)
            for row in meta_rows:
                log.track(8, " rowid=[" + str(row[0]) + "]  statid=[" + str(row[3]) + "]  pipo=[" + str(row[6]) + "]", True)

########################################################################
# Create the Float Message.
########################################################################
if cfg.tracking:
    log.track(0, "Create the Float Message.", True)

sm = BotMsg(cfg, log, 1)

# If there is a 'latest' Status Message from the previous search, pack
# the first one in the Result Set (that's the 'latest' Status Record
# which must be sent whether it has associated Data Products or not).
success = [ True, None, None ]
updstat = []
if stat_latest_results != None:
    if cfg.tracking:
        log.track(1, "Add 'Latest' Status Record with RowID #" + str(stat_latest_results[0][0]) + ".", True)

    success = sm.packstat(2, stat_latest_results[0])
    if success[0]:
        updstat.append(stat_latest_results[0][0])
        if cfg.tracking:
            log.track(1, "Remember this Status ID for future 'state' update.", True)
            log.track(2, "updstat: " + str(updstat), True)

# Cycle through all active/unsent Data Products and add to the Float
# message if they fit.
if success[0]:
    for row in meta_rows:
        # Check if Status Record associated with this Data Product was
        # just packed into this message (i.e., 'latest' Status Record)
        # or has already been sent to the Cloud. If not, we need to
        # send it in this message.
        statusID = stat_latest_results[0][0]
        statusFK = row[3]
        if cfg.tracking:
            log.track(1, "Process Data Product ID #" + str(row[0]), True)
            log.track(2, "Status ID: " + str(statusID), True)
            log.track(2, "Status FK: " + str(statusFK), True)
        if statusID == statusFK:
            if cfg.tracking:
                log.track(2, "FK Status Record Already Packed in THIS Message.", True)
        else:
            if cfg.tracking:
                log.track(2, "Get and Pack FK Status Record from DB.", True)

            sql = "SELECT rowid, * FROM status WHERE rowid = '" + str(statusFK) + "'"
            success, assoc_statrec = db.getResults(3, sql, False)

            if success[0]:
                if cfg.tracking:
                    log.track(3, "Got FK Status Record from DB; Pack into Message.", True)
                if assoc_statrec[0][1] == 0:
                    success = sm.packstat(4, assoc_statrec[0])
                    if success[0]:
                        updstat.append(assoc_statrec[0][0]) # Add to list of Status Recs
                        sentFlag = True     # Just Packed into THIS message.
                        if cfg.tracking:
                            log.track(3, "PACKED FK Status Record into Message.", True)
                            log.track(4, "updstat: " + str(updstat), True)
                    else:
                        if cfg.tracking:
                            log.track(3, "Can't PACK FK Status Record into Message; ", True)
            else:
                if cfg.tracking:
                    log.track(3, "Can't Get FK Status Record from DB; ", True)

            if not sentFlag:
                if cfg.tracking:
                    log.track(3, "FK Status Record Not Sent' Continue.", True)
                continue

            if cfg.tracking:
                log.track(2, "Get and Pack Data Product.", True)

            success = sm.packmeta(4, row)
            if success[0]:
                if cfg.tracking:
                    log.track(3, "PACKED Data Product Record into Message.", True)
            else:
                if cfg.tracking:
                    log.track(3, "Can't PACK Data Product Record.", True)

if cfg.tracking:
    log.track(1, "Final Message Complete.", True)
    log.track(2, "Buf Len: " + str(len(str(sm.buf))), True)
    log.track(2, "Buf Msg: " + str(sm.buf).encode("hex"), True)


sys.exit(0)

########################################################################
# Send the  Message Buffer.
########################################################################
if cfg.tracking:
    log.track(0, "Send the Message Buffer.", True)

bc = BotComm(cfg, log, "Iridium", 1)
success = bc.getconn(1)
if success[0]:
    success = bc.send(1, sm.buf)

success = bc.close(1)

########################################################################
# Close the Bot-Send Subsystem.
########################################################################
if cfg.tracking:
    log.track(0, "", True)
    log.track(0, "Bot-Send Subsystem Closing.", True)
    log.track(0, "", True)
sys.exit(0)

