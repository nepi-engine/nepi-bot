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
import socket
import sqlite3
import botdefs
from botcfg import BotCfg
from botlog import BotLog
from botdb import BotDB
from botmsg import BotMsg
from botpipo import BotPIPO
from botcomm import BotComm
from bothelp import getAllFolderNames, getAllFileNames
from bothelp import readFloatFile, triggerScoreLookup, resetCfgValue

########################################################################
# Instantiate a NEPI-Bot Configuration Class (from 'botcfg.py').
########################################################################

cfg = BotCfg()

########################################################################
##  Instantiate a Bot-Send Log File (from 'botlog.py').
########################################################################

log = BotLog(cfg, 0, False)    # False indicates bot-send (not bot-recv)
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

sql = "SELECT rowid, numerator, trigger, quality, score, metafile, norm FROM data WHERE state = '0'"
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

for dir in allfolders:
    # Get the ONLY sys_status.json file that should be in this Data
    # Product folder. Deconstruct it into a JSON object.
    data_prod_folder = cfg.data_dir_path + "/" + str(dir)
    status_file = data_prod_folder + "/" + cfg.sys_status_file
    if cfg.tracking:
        log.track(1, "Processing Status File", True)
        log.track(2, "Data Folder: " + str(data_prod_folder), True)
        log.track(2, "Status File: " + str(cfg.sys_status_file), True)

    acquired, status_json = readFloatFile(status_file, False, True)
    if cfg.tracking:
        log.track(2, "Acquired: " + str(acquired), True)
        log.track(7, "Contents: " + str(status_json), True)

    if not acquired:
        if cfg.tracking:
            log.track(1, "Status Record Not Acquired; Continue.", True)
        continue

    #-------------------------------------------------------------------
    # Insert Status Record and capture the DB 'rowid' (which is the DB
    # foreign key - or, pointer - back to this record in the DB which
    # will be embedded in all future Data Product records accociated
    # with this Status record.
    if cfg.tracking:
        log.track(2, "Insert Status Record into DB.", True)

    success, status_rowid = db.popstat(3, status_json)
    if cfg.tracking:
        log.track(3, "Row ID: " + str(status_rowid), True)

    if not success[0]:
        if cfg.tracking:
            log.errtrack(success[1], success[2])
        continue

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
    # investigating the mandatory 'Standard' Data File and and optional
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
        # the mandatory "Standard" and optional "Change" Data Files that
        # are identified in the Meta File).
        if cfg.tracking:
            log.track(4, "Get the PIPO Rating.", True)

        success, info = pipo.getPipo(5, meta_file, meta_json, trigger)
        if not success[0]:
            if cfg.tracking:
                log.track(4, "CAN'T PROCESS PIPO Rating; Continue.", True)
                continue

        #---------------------------------------------------------------
        # Put this Meta Data Product into the Float's Database.
        if cfg.tracking:
            log.track(4, "Put Meta Data Record into DB; Continue.", True)

        success = db.popdata(5, meta_json, info[3], status_rowid, trigger, info[0], info[1], info[2], info[4], info[5], meta_file)
    
########################################################################
# Create the  Message Buffer.
########################################################################
if cfg.tracking:
    log.track(0, "Create a Status Message.", True)

sm = BotMsg(cfg, log, 1, "STATUS")

success, msg_buff = sm.packhead(1, "SN_OoT_0001", "20190301_2")

success, results = db.getResults(1, """SELECT rowid, * FROM status WHERE state=0""", True)

for statrow in results:
    success, buff = sm.packstat(2, statrow)
    if success:
        msg_buff += str(buff)

########################################################################
# Send the  Message Buffer.
########################################################################
if cfg.tracking:
    log.track(0, "Send the Message Buffer.", True)

bc = BotComm(cfg, log, 1)
success = bc.getconn(1)
if success[0]:
    success = bc.sendmsg(1, msg_buff)

success = bc.close(1)

########################################################################
# Close the Bot-Send Subsystem.
########################################################################
if cfg.tracking:
    log.track(0, "", True)
    log.track(0, "Bot-Send Subsystem Closing.", True)
    log.track(0, "", True)
sys.exit(0)

