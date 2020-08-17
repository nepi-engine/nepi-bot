########################################################################
##
# Module: botmain.py
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

import binascii
import pdb
import argparse
import os
import sys
import time
import uuid
import json
import math
import ast
import struct
import zlib
import msgpack
import shutil
import socket
import sqlite3
from subprocess import Popen

from google.protobuf import json_format

import botdefs
from botdefs import nepi_home, bot_devnuid_file
from array import array
from botcfg import BotCfg
from botlog import BotLog
from botdb import BotDB
from botmsg import BotMsg
from botpipo import BotPIPO
from botcomm import BotComm
from bothelp import getAllFolderNames, getAllFileNames, getDevId
from bothelp import readFloatFile, triggerScoreLookup, resetCfgValue
from bothelp import writeFloatFile, resetCfgValue
from bothelp import deleteFolder, deleteDataProduct, create_nepi_dirs
import datetime
import nepi_messaging_all_pb2
from timestamp_pb2 import Timestamp

v_botmain = "bot71-20200601"

########################################################################
# Parse any command line options
########################################################################
testiplink = False
servermsg = object()
servermsgtype = int()
msgs_incoming = list()  # stack incoming messages for analysis and routing
msgs_outgoing = list()  # stack outgoing messages for transmittal to server

########################################################################
# Instantiate a NEPI-Bot Configuration Class (from 'botcfg.py')
########################################################################

cfg = BotCfg()
cfg.initcfg()

########################################################################
# Instantiate Bot-Main Debug/Log Object (from 'botlog.py')
########################################################################

log = BotLog(cfg, "BOT-MAIN", v_botmain)
log.initlog(0)

########################################################################
# Make Device ID available for all messaging.
########################################################################

dev_id_str, dev_id_bytes = getDevId(cfg, log, 0, bot_devnuid_file)

########################################################################
# Create necessary directories for messaging if they do not exist.
########################################################################

create_nepi_dirs(cfg, log, 3)

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
success = pipo.initPIPO(0)

if not success[0]:
    sys.exit(1)

########################################################################
# Instantiate a NEPI-Bot Message Class (from 'botmsg.py').
########################################################################

sm = BotMsg(cfg, log, 1)

########################################################################
# Re-Evaluate PIPO Ratings for Archived Data Products if Required.
########################################################################
log.track(1, "Launching botmain.py version " + v_botmain, True)

if cfg.tracking:
    log.track(0, "Recalculate Archived PIPO Ratings.", True)
    log.track(1, "Select 'Active' Data Records from Database.", True)

sql = "SELECT rowid, numerator, trigger, quality, score, timestamp, norm FROM data WHERE state = '0'"
success, rows = db.getResults(2, sql, False)

if success[0]:
    if not rows:
        log.track(1, "No Active Data Records in Database; Continue.", True)
    else:
        log.track(1, "Active Data Records in Database.", True)

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
            stmp = float(row[5])
            norm = float(row[6])
            if cfg.tracking:
                log.track(1, "Reevaluating Data Record ID: " + str(rwid), True)
                log.track(2, "Numr: " + str(numr), True)
                log.track(2, "Trig: " + str(trig), True)
                log.track(2, "Qual: " + str(qual), True)
                log.track(2, "Scor: " + str(scor), True)
                log.track(2, "Stmp: " + str(stmp), True)
                log.track(2, "Norm: " + str(norm), True)

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
            success, denominator = pipo.computeDenominator(3, stmp)

            if cfg.tracking:
                log.track(2, "Recalculcate PIPO Rating: ", False)
            pipo_rating = numerator / denominator
            if cfg.tracking:
                log.track(2, str(pipo_rating), True)

            sql = (
                "UPDATE data SET numerator="
                + str(numerator)
                + ", pipo="
                + str(pipo_rating)
                + " WHERE rowid="
                + str(rwid)
            )

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
# eligible for an uplink. The 'allfolders' array is guaranteed to be
# null if empty or on error.  The 'success' array can be safely ignored
# if desired since 'allfolders' reflects the correct meaning whether
# folders exist, don't exist, or an error occurs.  Sorting and Reversing

if cfg.tracking:
    log.track(0, "Get ALL Data Folders in Official Data Directory.", True)

success, allfolders = getAllFolderNames(cfg, log, 1, cfg.lb_data_dir_path, True, True)

########################################################################
# Retrieve New (All) Status and Data Product Files; Populate the DB.
########################################################################

haveNewStatus = 0

for dir in allfolders:
    # -------------------------------------------------------------------
    # Deal With Status Record First.
    # -------------------------------------------------------------------
    # Get the ONLY sys_status.json file that should be in THIS Data
    # Product folder. Deconstruct it into a JSON object and load into
    # the 'status' table of the embedded DB.  If any failure detected,
    # DESTROY the entire DP Folder since, without this Status Record,
    # all the Data Products in this DP Folder are useless.

    data_prod_folder = cfg.lb_data_dir_path + "/" + str(dir)
    status_file_path = data_prod_folder + "/" + cfg.sys_status_file
    if cfg.tracking:
        log.track(0, "", True)
        log.track(0, "Processing Status File.", True)
        log.track(1, "Data Folder: " + str(data_prod_folder), True)
        log.track(1, "Status File: " + str(cfg.sys_status_file), True)
        log.track(1, "Consume File from DP Directory.", True)

    success, status_json = readFloatFile(cfg, log, 2, status_file_path, False, True)

    print(status_json)

    if success[0]:
        # Insert Status Record and capture the DB 'rowid' (which will be
        # the DB foreign key - or, pointer - back to this record in the
        # DB, that is, embedded in all subsequent Data Product records
        # associated with this Status record).

        if cfg.tracking:
            log.track(1, "INSERT Status Record into DB 'status' Table.", True)

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
            log.track(2, "Consider Entire DP Folder Corrupt; Delete it.", True)

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

    # -------------------------------------------------------------------
    # Calculate Trigger Score
    # -------------------------------------------------------------------
    if cfg.tracking:
        log.track(1, "Calculate Trigger Score.", True)

    trigger, wet, wei = triggerScoreLookup(cfg, log, 2, status_json)
    if cfg.tracking:
        log.track(2, "Trigger Score:   " + str(trigger), True)
        log.track(2, "Wake Event Type: " + str(wet), True)
        log.track(2, "Wake Event ID: " + str(wei), True)

    # -------------------------------------------------------------------
    # Find All META Data Files
    # -------------------------------------------------------------------
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
        if "sys_status.json" in f:
            continue
        if ".json" in f:
            allmetafiles.append(f)

    if not allmetafiles:
        if cfg.tracking:
            log.track(2, "NO 'Meta' DP Files in this Data Folder.", True)
            log.track(2, "Done Processing; Remove Entire DP Folder.", True)

        deleteFolder(cfg, log, 3, data_prod_folder)
        if cfg.tracking:
            log.track(2, "Continue.", True)
        continue
        # log.track(2, "Update Status File with 'meta_state' of 1.", True)

        # sql = "UPDATE status SET meta_state=1 WHERE rowid=" + str(status_rowid)
        # success = db.update(3, sql)
        # if cfg.tracking:
        # log.track(2, "Continue.", True)
        # continue

    if cfg.tracking:
        log.track(2, "Got All 'Meta' DP Files.", True)
        log.track(3, "Files: " + str(allmetafiles), True)

    # -------------------------------------------------------------------
    # Process Each META File
    # -------------------------------------------------------------------
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

        # ---------------------------------------------------------------
        # Compute the PIPO Rating
        # ---------------------------------------------------------------
        # For each Meta File, get the PIPO rating (which also verifies
        # and returns the mandatory "Standard" and optional "Change"
        # Data Files that are identified in the Meta File).

        if cfg.tracking:
            log.track(2, "Get the PIPO Rating.", True)

        # TODO: Fix PIPO Rating
        #  success, info = pipo.getPIPO(3, meta_file_path, meta_json, trigger)
        # if not success[0]:
        #     if cfg.tracking:
        #         log.track(2, "Can't Compute PIPO rating.", True)
        #         log.track(2, "This DP is of No value; Remove.", True)
        #
        #     deleteDataProduct(cfg, log, 3, meta_file_path)
        #     if cfg.tracking:
        #         log.track(2, "Move on to next DP; Continue.", True)
        #     continue
        # numerator,
        # std_msg_size,
        # chg_msg_size,
        # chg_eligible,
        # norm,
        # pipo_rating,
        # std_file_path,
        # chg_file_path,
        # stdjson,
        # chgjson,
        # node_code,
        info = [1.0, 1500, 1500, 1, 1.0, 1.0, "DEF", "DEF", "", "", 0]
        # ---------------------------------------------------------------
        # INSERT the META Data Record into the Float's Database.
        # ---------------------------------------------------------------
        if cfg.tracking:
            log.track(2, "INSERT Meta Data Record into DB.", True)

        # success = db.pushmeta(3, meta_json, info[3], status_rowid, trigger, info[0], info[1], info[2], info[4], info[5], meta_file_path, stdfile, chgfile)
        success = db.pushdata(
            3, meta_json, info, status_rowid, trigger, meta_file_path, status_rowid
        )

        if not success[0]:
            if cfg.tracking:
                log.track(2, "Can't Insert Meta Record into DB." + str(mf), True)
                log.track(2, "This DP is of No value; Remove." + str(mf), True)

            deleteDataProduct(cfg, log, 3, meta_file_path)
            if cfg.tracking:
                log.track(2, "Move on to next DP; Continue.", True)
            continue

        # ---------------------------------------------------------------
        # DONE LOOP-PROCESSING NEW SDK DATA PRODUCTS.
        # ---------------------------------------------------------------

########################################################################
# Housekeeping of Data Directory.
########################################################################
# In theory, all the Data Products should have been processed if we get
# to this point.  In case of errors, those offending Data Products have
# already been removed.  Now, we should clean up any residue in the
# entire Data Directory.

if cfg.tracking:
    log.track(0, "Clean Up the Entire Data Directory.", True)
    log.track(1, "Get Remaining Data Folders in Data Dir.", True)

success, allfolders = getAllFolderNames(cfg, log, 2, cfg.lb_data_dir_path, True, True)

if not success[0]:
    if cfg.tracking:
        log.track(1, "UNABLE to perform Clean-Up.", True)
elif not allfolders:
    if cfg.tracking:
        log.track(1, "Nothing remaining to Clean-Up.", True)
else:
    for folder in allfolders:
        dir = cfg.lb_data_dir_path + "/" + str(folder)
        deleteFolder(cfg, log, 1, dir)

if cfg.tracking:
    log.track(1, "Clean-Up Completed; Continue.", True)

########################################################################
# Create the Float Message.
########################################################################
# If there is a 'latest' Status Message from the current wake-up search,
# pack the first one in the Result Set (that should be the latest Status
# Record which should be sent whether it has associated Data Products or
# not). Note that it is ordered by 'timestamp' to percolate the latest
# Status Record to the top of the list. If a new Status Record is found,
# it will be placed first in the uplink Message. Otherwise, skip this
# process and continue on processing both new or previoisly-stored Data
# Products.

if cfg.tracking:
    log.track(0, "Create the Float Message.", True)

if not haveNewStatus:
    if cfg.tracking:
        log.track(1, "No NEW Status Messages Processed; Skip This Step.", True)
else:
    if cfg.tracking:
        log.track(1, "Find 'Latest' Active Status Record for Bot Message.", True)

    sql = (
        "SELECT rowid, * FROM status WHERE state = '0' ORDER BY timestamp DESC LIMIT 1"
    )
    success, new_stat_results = db.getResults(2, sql, False)

    if success[0]:
        if not new_stat_results:
            if cfg.tracking:
                log.track(2, "Can't Find Expected 'Latest' Active SR in DB.", True)
                log.track(2, "Ignore; Continue with Data Products.", True)
        else:
            new_stat_rowid = new_stat_results[0][0]
            new_stat_state = new_stat_results[0][1]
            new_stat_stamp = float(new_stat_results[0][6])

            if cfg.tracking:
                log.track(2, "Found 'Latest' Active SR in Database.", True)
                log.track(3, "rowid=[" + str(new_stat_rowid) + "]", True)
                log.track(3, "state=[" + str(new_stat_state) + "]", True)
                log.track(3, "stamp=[" + str(new_stat_stamp) + "]", True)
                log.track(2, "Add This 'Latest' Status Record to Uplink Message.", True)

            # success = sm.packstat(3, new_stat_results[0])
            success = sm.encode_status_msg(3, new_stat_results[0], dev_id_bytes)
            if success[0]:
                if cfg.tracking:
                    log.track(1, "Latest Active SR Successfully Packed.", True)
                    log.track(1, "Update Status Record 'state' to 'packed.'", True)

                sql = (
                    "UPDATE status SET state = '1' WHERE rowid = '"
                    + str(new_stat_rowid)
                    + "'"
                )
                success = db.update(2, sql)
                if success[0]:
                    if cfg.tracking:
                        log.track(2, "Latest Active SR 'status Updated (Packed).", True)
                        log.track(2, "Continue now with Data Products.", True)
                else:
                    if cfg.tracking:
                        log.track(2, "Well ... This is Awkward.", True)
                        log.track(
                            2, "We May Eventually Repack This Same Status Record.", True
                        )
                        log.track(2, "Ignore; Continue with Data Products.", True)
            else:
                if cfg.tracking:
                    log.track(2, "Can't Pack 'Latest' Active Status Record.", True)
                    log.track(2, "Ignore; Continue with Data Products.", True)
    else:
        if cfg.tracking:
            log.track(2, "Can't Access 'Latest' Active Status Record from DB.", True)
            log.track(2, "Ignore; Continue with Data Products.", True)


########################################################################
# Select Active/Unsent Meta Records for the Float Message.
########################################################################

if cfg.tracking:
    log.track(0, "Select Top Active Data Products for Uplink Message.", True)

have_active_dp = False

sql = "SELECT rowid, * FROM data WHERE state = '0' ORDER BY pipo DESC LIMIT 32"
success, meta_rows = db.getResults(1, sql, False)

if success[0]:
    if not meta_rows:
        if cfg.tracking:
            log.track(1, "NO Active Data Products in Database; Continue.", True)
    else:
        have_active_dp = True
        if cfg.tracking:
            log.track(1, "Active Data Products FOUND in Database.", True)
            log.track(2, "Total: " + str(len(meta_rows)), True)
            for row in meta_rows:
                log.track(
                    14,
                    "rowid=["
                    + str(row[0])
                    + "]  statid=["
                    + str(row[3])
                    + "]  pipo=["
                    + str(row[6])
                    + "]",
                    True,
                )
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
            log.track(2, "Get Associated Status Record.", True)
            log.track(3, "Status FK: " + str(statusFK), True)

        # If not already packed in this Message, get it out of the 'status'
        # Table in DB. Then, pack this Status Record in the uplink Message
        # and save the Status Record's timestamp.
        # if not stat_sent_flag:
        sql = "SELECT rowid, * FROM status WHERE rowid = '" + str(statusFK) + "'"
        success, assoc_statrec = db.getResults(3, sql, False)
        if success[0]:
            if not assoc_statrec:
                if cfg.tracking:
                    log.track(3, "Can't Access Assoc Status Record in DB.", True)
                    log.track(3, "Likely a Previously-Sent Status Record.", True)
                    log.track(3, "Ignore; Continue with Next Data Product.", True)
                continue
            else:
                stat_state = assoc_statrec[0][1]
                stat_timestamp = assoc_statrec[0][6]

                if stat_state == 1:
                    if cfg.tracking:
                        log.track(3, "Assoc SR JUST PACKED into This Msg.", True)
                        log.track(4, "State: " + str(stat_state), True)
                        log.track(4, "Stamp: " + str(stat_timestamp), True)
                elif stat_state == 2:
                    if cfg.tracking:
                        log.track(3, "Assoc SR Previously Sent per DB.", True)
                        log.track(4, "State: " + str(stat_state), True)
                        log.track(4, "Stamp: " + str(stat_timestamp), True)
                else:
                    if cfg.tracking:
                        log.track(3, "Assoc SR FOUND in DB, but NOT Sent.", True)
                        log.track(4, "State: " + str(stat_state), True)
                        log.track(4, "Stamp: " + str(stat_timestamp), True)
                        log.track(2, "PACK it into THIS Uplink Message.", True)
                    success = sm.encode_status_msg(3, assoc_statrec[0],)
                    if success[0]:
                        if cfg.tracking:
                            log.track(3, "PACKED Assoc SR into This Message.", True)

                        sql = (
                            "UPDATE status SET state = '1' WHERE rowid = '"
                            + str(assoc_statrec[0][0])
                            + "'"
                        )

                        if cfg.tracking:
                            log.track(3, "Update Assoc SR 'state' to 'packed.'", True)

                        success = db.update(4, sql)
                        if success[0]:
                            if cfg.tracking:
                                log.track(3, "Assoc SR Status Updated (Packed).", True)
                        else:
                            if cfg.tracking:
                                log.track(3, "Well ... This is Awkward.", True)
                                log.track(
                                    3, "We May Eventually Repack This Same SR.", True
                                )
                                log.track(
                                    3, "Ignore; Continue with Next Data Product.", True
                                )
                            continue
        else:
            if cfg.tracking:
                log.track(3, "Assoc SR NOT Available from DB.", True)
                log.track(3, "Ignore; Continue with Next Data Product.", True)
            continue

        # ---------------------------------------------------------------
        # Get the Next 'index' value from 'node' Table in DB.
        # ---------------------------------------------------------------
        ntype = str(row[7])
        ninst = str(row[8])
        if cfg.tracking:
            log.track(2, "Get the Next 'index' value from 'node' Table in DB", True)

        sql = (
            "SELECT rowid, * FROM node WHERE node_type = '"
            + str(ntype)
            + "' AND node_instance = '"
            + str(ninst)
            + "' LIMIT 1"
        )
        success, node_results = db.getResults(3, sql, False)

        if not success[0] or not node_results:
            if cfg.tracking:
                log.track(
                    3,
                    "Inaccessible Node Type/Instance: " + str(ntype) + "/" + str(ninst),
                    True,
                )
                log.track(3, "Ignore This DP; Continue.", True)
            continue

        if not node_results:
            if cfg.tracking:
                log.track(
                    3,
                    "Node Type/Instance NOT Found: " + str(ntype) + "/" + str(ninst),
                    True,
                )
                log.track(3, "Ignore This DP; Continue.", True)
            continue

        node_index = int(node_results[0][4])
        node_stage = int(node_results[0][5])
        next_index = node_stage + 1

        if cfg.tracking:
            log.track(
                3, "Got Node Type/Instance: " + str(ntype) + "/" + str(ninst), True
            )
            log.track(4, "Index: " + str(node_index), True)
            log.track(4, "Stage: " + str(node_index), True)
            log.track(4, "Next:  " + str(next_index), True)

        # ---------------------------------------------------------------
        # Got Associated Status Record, so PACK this Data Product.
        # ---------------------------------------------------------------
        if cfg.tracking:
            log.track(2, "Pack This Data Product into Uplink Message.", True)

        success = sm.encode_data_msg(3, row, dev_id_bytes)  # stat_timestamp, next_index
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
                        log.track(
                            4, "We May Eventually Repack This Same Meta Record.", True
                        )
        else:
            if cfg.tracking:
                log.track(3, "Can't PACK Data Product Record.", True)
                continue

        # ---------------------------------------------------------------
        # Data Product Packed; Update 'node' Table in DB.
        # ---------------------------------------------------------------

        if cfg.tracking:
            log.track(1, "Update 'node' Table with latest Index.'", True)

            sql = (
                "UPDATE node SET node_stage = '"
                + str(next_index)
                + "' WHERE rowid = '"
                + str(node_results[0][0])
                + "'"
            )
            success = db.update(2, sql)
            if not success[0]:
                if cfg.tracking:
                    log.track(2, "Well ... This is Awkward.", True)
                    log.track(2, "Probably Come Back to Bite Us in the Ass.'", True)
                    log.track(
                        2, "Will wind up reverting back to 'std' DP Delivery.", True
                    )

if cfg.tracking:
    log.track(1, "Final Message Complete.", True)
    log.track(2, "Buf Len: " + str(len(str(sm.buf))), True)
    # TODO Check hex encoding
    # log.track(2, "Buf Msg: " + str(sm.buf).encode("hex"), True)
    # log.track(2, "Buf Msg: " + str(binascii.hexlify(str.encode(sm.buf))), True)

########################################################################
# Open Communications Port and Send the Message Buffer.
########################################################################

if cfg.tracking:
    log.track(0, "Send the Message Buffer.", True)

# TODO REMOVE BEFORE PRODUCTION
# if testiplink:
#     sm.buf = "THIS A ENCRYPTED IP LINK TEST TO CRITIGEN TEST SERVER"
#     sm.len = len(sm.buf)

# bcsuccess = 0
# cnc_msgs = list()


def get_enabled_link(_cfg):
    for link in _cfg.lb_conn_order:
        enabled = _cfg[link]["enabled"]
        if enabled:
            yield link
    return None


# AGV
# xtype = get_enabled_link(cfg)
bc = BotComm(cfg, log, "ethernet", 1)
success = bc.getconn(0)
# AGV

if sm.len > 0:
    # AGV
    # xtype = get_enabled_link(cfg)
    # bc = BotComm(cfg, log, xtype, 1)
    # success = bc.getconn(0)
    # AGV

    if success[0]:
        log.track(0, "Sending: " + str(sm.buf), True)
        send_success, cnc_msgs = bc.send(1, sm.buf, 5)
        if send_success[0]:
            log.track(0, "send returned Success", True)
            bcsuccess = 1  # Added as gap fix for no scuttle
        else:
            log.track(0, "send returned Not Success", True)
            bcsuccess = 0  # Added as gap fix for no scuttle
    else:
        send_success = [False, None, None]
        cnc_msgs = None
        log.track(0, "getconn returned Not Success", True)
        bcsuccess = 0  # Added as gap fix for no scuttle
else:
    if cfg.tracking:
        log.track(0, "NO Uplink Message to Send.", True)

    # Receive messages from server
recv_success, cnc_msgs = bc.receive(1, 1)
if recv_success[0]:
    log.track(0, "receive returned Success", True)
    log.track(0, "Received: " + str(cnc_msgs[0]), True)
    bcsuccess = 1
else:
    recv_success = [False, None, None]
    success = bc.close(1)

success = bc.close(1)

########################################################################
# Make sure any downlinked commands get processed.
########################################################################
sdk_action = False
# Botcomm indicates no cnc_msgs with either None or an empty list.
if cnc_msgs is None:
    cnc_msgs = list()
for msgnum in range(0, len(cnc_msgs)):
    # msg_b64 = cnc_msgs[msgnum]
    # msg = msg_b64.decode('base64')
    if cfg.devclass == "generic":
        msg = cnc_msgs[msgnum]
        msg_pos = 0
        msg_len = len(msg)
        # msg_hex = str(msg).encode('hex')
        if cfg.tracking:
            log.track(1, "Evaluating C&C Message #" + str(msgnum), True)
            log.track(2, "msg_pos: [" + str(msg_pos) + "]", True)
            log.track(2, "msg_len: [" + str(msg_len) + "]", True)
            # log.track(
            #    14, "msg_hex: [" + str(msg_hex) + "] <-- s/b double.", True)
    else:
        msg = cnc_msgs[msgnum]
        msg_pos = 0
        msg_len = len(msg)
        msg_hex = str(msg).encode("hex")
        if cfg.tracking:
            log.track(1, "Evaluating C&C Message #" + str(msgnum), True)
            log.track(2, "msg_pos: [" + str(msg_pos) + "]", True)
            log.track(2, "msg_len: [" + str(msg_len) + "]", True)
            # log.track(14, "msg_hex: [" + str(msg_hex) + "] <-- s/b double.", True)
    # -------------------------------------------------------------------
    # Loop Through the C&C Segments Until Message is Exhausted.
    # ------------------------------------------------------------------_
    # TODO Handle more than one message in packet

    if cfg.devclass == "generic":
        servermsg = nepi_messaging_all_pb2.NEPIMsg()
        # servermsg_len = servermsg.ParseFromString(msg)
        new_msg_json = json_format.MessageToJson(
            servermsg, preserving_proto_field_name=False, indent=2
        )
        print(new_msg_json)
    # else:
    #     while msg_pos < msg_len:
    #         # Even though msg 'header' is exactly 3 bytes, peel off 4 so
    #         # 'struct' can deal with it as a 32-bit integer. Ignore the
    #         # far-right eight bits (that's atually the 1st byte of data).
    #         if cfg.tracking:
    #             log.track(2, "Evaluating Segment Header.", True)
    #         try:
    #             seg_head = struct.unpack(">I", msg[msg_pos : msg_pos + 4])[0]
    #             # 'protocol'    (bits 0-1)
    #             seg_prot = (seg_head & 0xC0000000) >> 30
    #             # 'config type' (bits 2-4)
    #             seg_type = (seg_head & 0x38000000) >> 27
    #             # 'msg length'  (bits 5-15)
    #             seg_size = (seg_head & 0x07FF0000) >> 16
    #             # 'cfg index'   (bits 16-21)
    #             seg_indx = (seg_head & 0x0000FC00) >> 10
    #             # 'msg flags'   (bits 22-23)
    #             seg_flag = (seg_head & 0x00000300) >> 8
    #         except Exception as e:
    #             if cfg.tracking:
    #                 log.track(3, "Lost Control Evaluating This Message.", True)
    #                 log.track(3, "ERROR: [" + str(e) + "]", True)
    #                 log.track(3, "Continue w/next MESSAGE.", True)
    #             break
    #         if cfg.tracking:
    #             log.track(15, "Starting:  [" + str(int(msg_pos)) + "]", True)
    #             log.track(15, "seg_prot:  [" + str(int(seg_prot)) + "]", True)
    #             log.track(15, "seg_type:  [" + str(int(seg_type)) + "]", True)
    #             log.track(15, "seg_size:  [" + str(int(seg_size)) + "]", True)
    #             log.track(15, "seg_indx:  [" + str(int(seg_indx)) + "]", True)
    #             log.track(15, "seg_flag:  [" + str(int(seg_flag)) + "]", True)
    #         # ---------------------------------------------------------------
    #         # Position at Data and Decompress/Unpack.
    #         # ---------------------------------------------------------------
    #         msg_pos += 3  # Skip forward over the 3-byte segment 'header.'
    #         if cfg.tracking:
    #             log.track(2, "Bump Forward to Process Message Data.", True)
    #             log.track(15, "msg_pos:  [" + str(msg_pos) + "]", True)
    #             log.track(2, "Perform 'struct' Unpacking.", True)
    #         try:
    #             data_fmt = ">" + str(seg_size) + "s"
    #             if cfg.tracking:
    #                 log.track(15, "data_fmt: [" + str(data_fmt) + "]", True)
    #             data_raw = msg[msg_pos : msg_pos + seg_size]
    #             if cfg.tracking:
    #                 log.track(15, "data_raw: [" + str(data_raw) + "]", True)
    #             data_len = len(data_raw)
    #             if cfg.tracking:
    #                 log.track(15, "data_len: [" + str(data_len) + "]", True)
    #             data_hex = data_raw.encode("hex")
    #             if cfg.tracking:
    #                 log.track(15, "data_hex: [" + str(data_hex) + "]", True)
    #             if seg_type == 0 and (int(seg_flag) != 1):  # If Cmd, grab byte 1 of
    #                 # data in its hex value.
    #                 data_unp = data_hex
    #             else:
    #                 data_unp = struct.unpack(data_fmt, data_raw)[0]
    #                 if cfg.tracking:
    #                     log.track(15, "data_unp: [" + str(data_unp) + "]", True)
    #                 if cfg.data_zlib:  # Are we using 'zlib' compression?
    #                     if cfg.tracking:
    #                         log.track(2, "Perform 'zlib' Decompression.", True)
    #                     decompress = zlib.decompressobj(-zlib.MAX_WBITS)
    #                     data_dcmp = decompress.decompress(data_raw)
    #                     data_dcmp += decompress.flush()
    #                     if cfg.tracking:
    #                         log.track(15, "data_dcmp: [" + str(data_dcmp) + "]", True)
    #                     data_dsiz = len(data_dcmp)
    #                     if cfg.tracking:
    #                         log.track(15, "data_dsiz: [" + str(data_dsiz) + "]", True)
    #                     data_unp = data_dcmp
    #                 if cfg.data_msgpack:  # Are we using 'msgpack' unpacking?
    #                     if cfg.tracking:
    #                         log.track(2, "Perform 'msgpack' Unpacking.", True)
    #                     data_mpak = msgpack.unpackb(data_dcmp)
    #                     if cfg.tracking:
    #                         log.track(16, "data_mpak: [" + str(data_mpak) + "]", True)
    #                     data_mlen = len(str(data_mpak))
    #                     if cfg.tracking:
    #                         log.track(16, "data_mlen: [" + str(data_mlen) + "]", True)
    #                     data_unp = data_mpak
    #             if cfg.tracking:
    #                 log.track(15, "data_unp: [" + str(data_unp) + "]", True)
    #         except Exception as e:
    #             if cfg.tracking:
    #                 log.track(3, "Problem(s) Processing Data Segment.", True)
    #                 log.track(3, "ERROR: [" + str(e) + "]", True)
    #                 log.track(3, "Continue w/next MESSAGE.", True)
    #             break
    #     # ---------------------------------------------------------------
    #     # Process the Bot Config, Command, or SDK Config Messages.
    #     # ---------------------------------------------------------------
    #
    #     if cfg.devclass == "generic":
    #         # TODO add index
    #
    #         if servermsg.hasattr(msg, "CfgMsg"):
    #             servermsgtype = 1
    #
    #         elif servermsg.hasattr(msg, "GeneralMsg"):
    #             pass
    #     else:
    #         if int(seg_flag) == 1:  # --------------------------- BOT CONFIG
    #             if cfg.tracking:
    #                 log.track(2, "Process BOT CONFIG.", True)
    #             for key in data_unp:
    #                 val = data_unp[key]
    #                 if cfg.tracking:
    #                     log.track(3, "Process keyword: [" + str(key) + "]", True)
    #                     log.track(4, "val: [" + str(val) + "]", True)
    #                 if str(key) == "scor":
    #                     nkey = "pipo_scor_wt"
    #                 elif str(key) == "qual":
    #                     nkey = "pipo_qual_wt"
    #                 elif str(key) == "size":
    #                     nkey = "pipo_size_wt"
    #                 elif str(key) == "trig":
    #                     nkey = "pipo_trig_wt"
    #                 elif str(key) == "time":
    #                     nkey = "pipo_time_wt"
    #                 elif str(key) == "msg_size":
    #                     nkey = "max_msg_size"
    #                 elif str(key) == "type":
    #                     nkey = "type"
    #                 elif str(key) == "host":
    #                     nkey = "host"
    #                 elif str(key) == "port":
    #                     nkey = "port"
    #                 elif str(key) == "baud":
    #                     nkey = "baud"
    #                 elif str(key) == "p_size":
    #                     nkey = "packet_size"
    #                 else:
    #                     if cfg.tracking:
    #                         log.track(
    #                             3,
    #                             "Unknown 'keyword' received: [" + str(key) + "]",
    #                             True,
    #                         )
    #                         log.track(3, "Continue w/next KEYWORD.", True)
    #                     msg_pos += seg_size
    #                     continue
    #                 if cfg.tracking:
    #                     log.track(4, "CFG: [" + str(nkey) + "]", True)
    #                 log.track(3, "Update the Bot Config File.", True)
    #                 resetCfgValue(cfg, log, 4, str(nkey), val)
    #             if cfg.tracking:
    #                 log.track(3, "DONE with Bot Config Updates.", True)
    #                 log.track(3, "Continue w/next SEGMENT.", True)
    #             msg_pos += seg_size
    #             continue
    #         elif seg_type == 0:  # --------------------------- COMMAND
    #             if cfg.tracking:
    #                 log.track(2, "Process COMMAND.", True)
    #                 log.track(3, "Identify Directory and Base Name.", True)
    #             fpath = "/commands/"
    #             cmd = int(data_unp)
    #             if cmd == 1:
    #                 fname = "scuttle"  # Scuttle
    #             elif cmd == 2:
    #                 fname = "ping"  # Ping
    #             else:
    #                 if cfg.tracking:
    #                     log.track(
    #                         3,
    #                         "WARNING: Got 'Unknown' Command: [" + str(cmd) + "]",
    #                         True,
    #                     )
    #                     log.track(
    #                         3, "No Implementation; Continue w/next SEGMENT.", True
    #                     )
    #                 msg_pos += seg_size
    #                 continue
    #         else:  # ------------------------------------------- CONFIGURATION
    #             if cfg.tracking:
    #                 log.track(2, "Process CONFIGURATION.", True)
    #                 log.track(3, "Identify Directory and Base File Name.", True)
    #             if seg_type == 1:  # SENSOR
    #                 fpath = "/cfg/sensors/"
    #                 fname = "sensor_cfg"
    #             elif seg_type == 2:  # NODE
    #                 fpath = "/cfg/proc_nodes/"
    #                 fname = "proc_node_cfg"
    #             elif seg_type == 3:  # GEOFENCE
    #                 fpath = "/cfg/geofence/"
    #                 fname = "geofence_cfg"
    #             elif seg_type == 4:  # RULE
    #                 fpath = "/cfg/rules/"
    #                 fname = "smarttrig_rule"
    #             elif seg_type == 5:  # TRIGGER
    #                 fpath = "/cfg/trig/"
    #                 fname = "smarttrig_cfg"
    #             elif seg_type == 6:  # ACTION
    #                 fpath = "/cfg/action/"
    #                 fname = "action_seq"
    #             elif seg_type == 7:  # SCHEDULE
    #                 fpath = "/cfg/sched/"
    #                 fname = "task"
    #             else:
    #                 if cfg.tracking:
    #                     log.track(2, "WARNING: Got 'Unknown' C&C Segment TYPE.", True)
    #                     log.track(
    #                         2, "No Implementation; Continue w/next SEGMENT.", True
    #                     )
    #                 msg_pos += seg_size
    #                 continue
    #         if cfg.tracking:
    #             log.track(4, "bpath: [" + str(fpath) + "]", True)
    #             log.track(4, "bname: [" + str(fname) + "]", True)
    #             log.track(3, "Construct File Path, Name, and File.", True)
    #         try:
    #             fpath = str(nepi_home) + str(fpath)
    #             if cfg.tracking:
    #                 log.track(3, "fpath: [" + str(fpath) + "]", True)
    #             if seg_type > 0:
    #                 fname = str(fname) + "_" + str(seg_indx).zfill(5) + ".json"
    #             if cfg.tracking:
    #                 log.track(3, "fname: [" + str(fname) + "]", True)
    #             ffile = str(fpath) + str(fname)
    #             if cfg.tracking:
    #                 log.track(3, "ffile: [" + str(ffile) + "]", True)
    #             if seg_type > 0:
    #                 # This can be used if the JSON requires 'expansion.'
    #                 # fpars = json.loads(data_unp)
    #                 # if cfg.tracking:
    #                 # log.track(15, "fpars: [" + str(fpars) + "]", True)
    #                 fdump = json.dumps(data_unp, indent=4, sort_keys=False)
    #                 if cfg.tracking:
    #                     log.track(15, "fdump: [" + str(fdump) + "]", True)
    #             else:
    #                 fpars = ""
    #                 fdump = ""
    #         except Exception as e:
    #             if cfg.tracking:
    #                 log.track(3, "Problem(s) Constructing C&C File.", True)
    #                 log.track(3, "ERROR: [" + str(e) + "]", True)
    #                 log.track(3, "Continue w/next SEGMENT.", True)
    #             msg_pos += seg_size
    #             continue
    #         if cfg.tracking:
    #             log.track(2, "Create Config File for the SDK.", True)
    #         success = writeFloatFile(cfg, log, 3, True, ffile, str(fdump))
    #         if success[0]:
    #             sdk_action = True  # Got at least 1 C&C message for the SDK.
    #         if cfg.tracking:
    #             log.track(2, "Continue w/next SEGMENT.", True)
    #         msg_pos += seg_size
    #         continue
########################################################################
# SDK C&C Downlink Message Notification.
########################################################################
if cfg.tracking:
    log.track(0, "SDK C&C Downlink Message Notification.", True)
if sdk_action:
    if cfg.tracking:
        log.track(1, "Messages Processed; Proceed.", True)
    if cfg.devclass == "float1":
        sdkproc = "/opt/numurus/ros/nepi-utilities/process-updates.sh"
        sdkwhat = "Live Float Mode"
    else:
        sdkproc = str(nepi_home) + "/src/tst/sdktest.sh"
        sdkwhat = "Local Non-Float Mode."
    try:
        if cfg.tracking:
            log.track(1, str(sdkwhat), True)
            log.track(1, "File: [" + str(sdkproc) + "]", True)
            log.track(1, "Setting 'devnull' device", True)
        devnull = open(os.devnull, "wb")
        if cfg.tracking:
            log.track(1, "Execute Popen w/nohup.", True)
        log.track(1, "Looking for sdkproc at: {}".format(str(sdkproc)), True)
        if os.path.isfile(str(sdkproc)):
            # Popen([str(sdkproc)])
            # Popen(['nohup', str(sdkproc)])
            proc = Popen(
                ["nohup", "/bin/sh", str(sdkproc)], stdout=devnull, stderr=devnull
            )
            rc = proc.wait()
            log.track(1, "{} returned {}".format(str(sdkproc), rc), True)
        else:
            log.track(1, os.listdir("/opt/numurus/ros/nepi-utilities/"), True)
            raise ValueError("Can't find SDK Notification Process.")
        if cfg.tracking:
            log.track(1, "DONE with SDK Notification.", True)
    except Exception as e:
        if cfg.tracking:
            log.track(2, "Errors(s) Executing Local Test App.", True)
            log.track(2, "ERROR: [" + str(e) + "]", True)
            log.track(2, "Continue.", True)
else:
    if cfg.tracking:
        log.track(1, "NO Messages Processed: Ignore SDK Notification.", True)


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
            log.track(
                1, "Reset Bit-Packed Status Record(s) to 'new/active' Status.", True
            )

        sql = "UPDATE status SET state = '0' WHERE state = '1'"
        success = db.update(2, sql)
        if not success[0]:
            if cfg.tracking:
                log.track(2, "Well ... This is Awkward.", True)
        else:
            if cfg.tracking:
                log.track(2, "Done.", True)

        if cfg.tracking:
            log.track(
                1, "Reset Bit-Packed Meta Record(s) to 'new/active' Status.", True
            )

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
        log.track(1, "NO Message = NO Housekeeping to be Done.", True)

success = db.close(0)

########################################################################
# Close the Bot-Main Subsystem.
########################################################################

if cfg.tracking:
    log.track(0, "", True)
    log.track(0, "Bot-Main Subsystem Closing.", True)
    log.track(0, "", True)

if not bcsuccess:  # Added as gap fix for no scuttle
    log.track(0, "Bot-Main Sending Not Success.", True)
    sys.exit(1)  # Added as gap fix for no scuttle
else:
    log.track(0, "Bot-Main Sending Success.", True)
    sys.exit(0)
