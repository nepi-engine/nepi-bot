########################################################################
##
# Module: botlbproc.py
# --------------------
##
# (c) Copyright 2021 by Numurus LLC
##
# This document, and all information therein, is the property of
# Numurus LLC.  It is confidential and must not be made public or
# copied in any form.  It is loaned subject to return upon demand
# and is not to be used directly or indirectly in any way detrimental
# to our interests.
##
########################################################################
import json
import operator
import os
import pathlib
from pathlib import Path

import botdefs
from botcomm import BotComm
from bothelp import (
    resetCfgValue,
    getAllFolderNames,
    readFloatFile,
    deleteFolder,
    triggerScoreLookup,
    getAllFileNames,
    deleteDataProduct,
)
import botreports

v_botlbproc = "bot71-20210205"


class LbProc(object):
    def __init__(
            self,
            _cfg,
            _log,
            _lev,
            _dev_id_str,
            _dev_id_bytes,
            _nepi_args,
            _db,
            _pipo,
            _sm,
            _v_botmain,
    ):
        # global gen_msg_contents

        self.haveNewStatus = 0
        self.cfg = _cfg
        self.log = _log
        self.lev = _lev
        self.dev_id_str = _dev_id_str
        self.dev_id_bytes = _dev_id_bytes
        self.nepi_args = _nepi_args
        self.db = _db
        self.pipo = _pipo
        self.sm = _sm
        self.v_botmain = _v_botmain
        self.gen_msg_contents = ""
        self.original_dir = Path.cwd()
        self.lb_dir = os.path.abspath(botdefs.bot_hb_dir)
        self.ssh_key_file = os.path.abspath(botdefs.bot_devsshkeys_file)
        self.msgs_incoming = botdefs.msgs_incoming
        self.msgs_outgoing = botdefs.msgs_outgoing
        self.nepi_home = botdefs.nepi_home
        self.rpt_items = []
        self.log.track(self.lev, "Created LbProc Class Object.", True)

    def lb_process_data(self):

        if self.cfg.db_deletes:
            # remove sent metadata records from database
            sql = "DELETE FROM data WHERE rec_state = 2"
            success = self.db.update(2, sql)
            if success[0]:
                self.log.track(0, "Deleted SENT metadata records from database.", True)
            else:
                self.log.track(0, "Unable to delete SENT metadata records from database.", True)

            # remove sent status records from database
            sql = "DELETE FROM status WHERE rec_state = 2"
            success = self.db.update(2, sql)
            if success[0]:
                self.log.track(0, "Deleted SENT status records from database.", True)
            else:
                self.log.track(0, "Unable to delete SENT status records from database.", True)

        if self.cfg.tracking:
            self.log.track(0, "Recalculate Archived PIPO Ratings.", True)
            self.log.track(1, "Select 'Active' Data Records from Database.", True)

        sql = "SELECT rowid, numerator, trigger, quality, event_score, timestamp, type_score FROM data WHERE rec_state in (0,1)"
        success, rows = self.db.getResults(2, sql, False)

        if success[0]:
            if not rows:
                self.log.track(1, "No Active Data Records in Database; Continue.", True)
            else:
                self.log.track(1, "Active Data Records in Database.", True)

                if self.cfg.wt_changed:
                    if self.cfg.tracking:
                        self.log.track(1, "Weight Factors Have Changed.", True)
                else:
                    if self.cfg.tracking:
                        self.log.track(1, "Weight Factors Are Unchanged.", True)

                for row in rows:
                    rwid = int(row[0])
                    numr = float(row[1])
                    trig = float(row[2])
                    qual = float(row[3])
                    scor = float(row[4])
                    stmp = float(row[5])
                    norm = float(row[6])
                    if self.cfg.tracking:
                        self.log.track(
                            1, "Reevaluating Data Record ID: " + str(rwid), True
                        )
                        self.log.track(2, "Numr: " + str(numr), True)
                        self.log.track(2, "Trig: " + str(trig), True)
                        self.log.track(2, "Qual: " + str(qual), True)
                        self.log.track(2, "Scor: " + str(scor), True)
                        self.log.track(2, "Stmp: " + str(stmp), True)
                        self.log.track(2, "Norm: " + str(norm), True)

                    if self.cfg.wt_changed:
                        if self.cfg.tracking:
                            self.log.track(2, "Recalculate Numerator.", True)
                        success, numerator = self.pipo.computeNumerator(
                            3, scor, qual, norm, trig
                        )
                    else:
                        if self.cfg.tracking:
                            self.log.track(
                                2, "Use existing Numerator." + str(numr), True
                            )
                        numerator = numr

                    if self.cfg.tracking:
                        self.log.track(2, "Recalculate Denominator.", True)
                    success, denominator = self.pipo.computeDenominator(3, stmp)

                    if self.cfg.tracking:
                        self.log.track(2, "Recalculcate PIPO Rating: ", False)
                    pipo_rating = numerator / denominator
                    if self.cfg.tracking:
                        self.log.track(2, str(pipo_rating), True)

                    sql = (
                            "UPDATE data SET numerator="
                            + str(numerator)
                            + ", pipo="
                            + str(pipo_rating)
                            + " WHERE rowid="
                            + str(rwid)
                    )

                    success = self.db.update(2, sql)

                if self.cfg.wt_changed:
                    if self.cfg.tracking:
                        self.log.track(
                            1,
                            "All Archive PIPOs Recalculated; Reset Config JSON.",
                            True,
                        )

                    resetCfgValue(self.cfg, self.log, 2, "wt_changed", 0)

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

        if self.cfg.tracking:
            self.log.track(0, "Get ALL Data Folders in Official Data Directory.", True)

        success, allfolders = getAllFolderNames(
            self.cfg, self.log, 1, self.cfg.lb_data_dir_path, True, True
        )

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

            data_prod_folder = self.cfg.lb_data_dir_path + "/" + str(dir)
            status_file_path = data_prod_folder + "/" + self.cfg.sys_status_file
            if self.cfg.tracking:
                self.log.track(0, "", True)
                self.log.track(0, "Processing Status File.", True)
                self.log.track(1, "Data Folder: " + str(data_prod_folder), True)
                self.log.track(1, "Status File: " + str(self.cfg.sys_status_file), True)
                self.log.track(1, "Consume File from DP Directory.", True)

            success, status_json = readFloatFile(
                self.cfg, self.log, 2, status_file_path, False, True
            )

            if success[0]:
                # Insert Status Record and capture the DB 'rowid' (which will be
                # the DB foreign key - or, pointer - back to this record in the
                # DB, that is, embedded in all subsequent Data Product records
                # associated with this Status record).

                if self.cfg.tracking:
                    self.log.track(
                        1, "INSERT Status Record into DB 'status' Table.", True
                    )

                success, status_rowid = self.db.pushstat(2, status_json)
                if not success[0]:
                    if self.cfg.tracking:
                        self.log.track(2, "Status Record Insertion Failed.", True)
                        self.log.track(2, "Entire DP Folder Unusable; Delete it.", True)

                    deleteFolder(self.cfg, self.log, 3, data_prod_folder)
                    if self.cfg.tracking:
                        self.log.track(2, "Continue.", True)
                    continue

            else:
                if self.cfg.tracking:
                    self.log.track(2, "Status File NOT Acquired.", True)
                    self.log.track(
                        2, "Consider Entire DP Folder Corrupt; Delete it.", True
                    )

                deleteFolder(self.cfg, self.log, 3, data_prod_folder)
                if self.cfg.tracking:
                    self.log.track(2, "Continue.", True)
                continue

            # Retain this 'latest' Status Record; these will be retrieved later
            # in order to have at least one placed first in the uplink message.
            if haveNewStatus == 0:
                haveNewStatus = status_rowid
                if self.cfg.tracking:
                    self.log.track(
                        2, "Have at Least 1 New Status Record for Uplink Msg.", True
                    )
                    self.log.track(3, "ID: " + str(haveNewStatus), True)

            # -------------------------------------------------------------------
            # Calculate Trigger Score
            # -------------------------------------------------------------------
            if self.cfg.tracking:
                self.log.track(1, "Calculate Trigger Score.", True)

            trigger, wet, wei = triggerScoreLookup(self.cfg, self.log, 2, status_json)
            if self.cfg.tracking:
                self.log.track(2, "Trigger Score:   " + str(trigger), True)
                self.log.track(2, "Wake Event Type: " + str(wet), True)
                self.log.track(2, "Wake Event ID: " + str(wei), True)

            # -------------------------------------------------------------------
            # Find All META Data Files
            # -------------------------------------------------------------------
            # The 'sys_status.json' file for this Data Folder is now in the DB;
            # we need to find ALL Meta Data files (*_meta.json) in this same
            # Folder (the Status File we're processing right now applies to all
            # Meta Files in this same Data Folder).
            if self.cfg.tracking:
                self.log.track(1, "Get ALL DP Files in This Data Folder.", True)
                self.log.track(2, "Folder:" + data_prod_folder, True)

            success, allfiles = getAllFileNames(
                self.cfg, self.log, 2, data_prod_folder, False, False
            )
            if not success[0] or not allfiles:
                if self.cfg.tracking:
                    self.log.track(2, "Data Folder File NOT ACCESSIBLE.", True)
                    self.log.track(2, "Remove Entire DP Folder.", True)

                deleteFolder(self.cfg, self.log, 3, data_prod_folder)
                if self.cfg.tracking:
                    self.log.track(2, "Continue.", True)
                continue

            # Retrieve all "meta" files out of the list.
            allmetafiles = []
            for f in allfiles:
                if "sys_status.json" in f:
                    continue
                if ".json" in f:
                    allmetafiles.append(f)

            if not allmetafiles:
                if self.cfg.tracking:
                    self.log.track(2, "NO 'Meta' DP Files in this Data Folder.", True)
                    self.log.track(2, "Done Processing; Remove Entire DP Folder.", True)

                deleteFolder(self.cfg, self.log, 3, data_prod_folder)
                if self.cfg.tracking:
                    self.log.track(2, "Continue.", True)
                continue
                # log.track(2, "Update Status File with 'meta_state' of 1.", True)

                # sql = "UPDATE status SET meta_state=1 WHERE rowid=" + str(status_rowid)
                # success = db.update(3, sql)
                # if cfg.tracking:
                # log.track(2, "Continue.", True)
                # continue

            if self.cfg.tracking:
                self.log.track(2, "Got All 'Meta' DP Files.", True)
                self.log.track(3, "Files: " + str(allmetafiles), True)

            # -------------------------------------------------------------------
            # Process Each META File
            # -------------------------------------------------------------------
            for mf in allmetafiles:
                # First, compose the full path to the Meta File.
                meta_file_path = data_prod_folder + "/" + mf
                if self.cfg.tracking:
                    self.log.track(1, "Processing META File: " + str(mf), True)
                    self.log.track(2, "Path: " + str(data_prod_folder), True)
                    self.log.track(2, "Consume File from DP Directory.", True)

                # Now process each Meta File by calculating its PIPO Rating,
                # which evaluates the mandatory 'Standard' Data File and any
                # optional 'Change' Data File.
                success, meta_json = readFloatFile(
                    self.cfg, self.log, 3, meta_file_path, False, True
                )
                if not success[0]:
                    if self.cfg.tracking:
                        self.log.track(
                            2, "Can't Access the Data Product's 'Meta' File.", True
                        )
                        self.log.track(
                            2, "These DP Files are Useless; Remove Them.", True
                        )

                    deleteDataProduct(self.cfg, self.log, 3, meta_file_path)
                    if self.cfg.tracking:
                        self.log.track(2, "Move on to next DP; Continue.", True)
                    continue

                # ---------------------------------------------------------------
                # Compute the PIPO Rating
                # ---------------------------------------------------------------
                # For each Meta File, get the PIPO rating (which also verifies
                # and returns the mandatory "Standard" and optional "Change"
                # Data Files that are identified in the Meta File).

                if self.cfg.tracking:
                    self.log.track(2, "Get the PIPO Rating.", True)

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
                if self.cfg.tracking:
                    self.log.track(2, "INSERT Meta Data Record into DB.", True)

                # success = db.pushmeta(3, meta_json, info[3], status_rowid, trigger, info[0], info[1], info[2], info[4], info[5], meta_file_path, stdfile, chgfile)
                success = self.db.pushdata(
                    3,
                    meta_json,
                    info,
                    status_rowid,
                    trigger,
                    meta_file_path,
                    status_rowid,
                )

                if not success[0]:
                    if self.cfg.tracking:
                        self.log.track(
                            2, "Can't Insert Meta Record into DB." + str(mf), True
                        )
                        self.log.track(
                            2, "This DP is of No value; Remove." + str(mf), True
                        )

                    deleteDataProduct(self.cfg, self.log, 3, meta_file_path)
                    if self.cfg.tracking:
                        self.log.track(2, "Move on to next DP; Continue.", True)
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

        if self.cfg.tracking:
            self.log.track(0, "Clean Up the Entire Data Directory.", True)
            self.log.track(1, "Get Remaining Data Folders in Data Dir.", True)

        success, allfolders = getAllFolderNames(
            self.cfg, self.log, 2, self.cfg.lb_data_dir_path, True, True
        )

        if not success[0]:
            if self.cfg.tracking:
                self.log.track(1, "UNABLE to perform Clean-Up.", True)
        elif not allfolders:
            if self.cfg.tracking:
                self.log.track(1, "Nothing remaining to Clean-Up.", True)
        else:
            for folder in allfolders:
                tdir = self.cfg.lb_data_dir_path + "/" + str(folder)
                deleteFolder(self.cfg, self.log, 1, tdir)

        if self.cfg.tracking:
            self.log.track(1, "Clean-Up Completed; Continue.", True)

        # Terminate early if env var RUN_LB_LINK is False.
        if not self.nepi_args.lb:
            if self.cfg.tracking:
                self.log.track(1, "LB Data not requested to be sent to server. LB terminating early after storing status/data records.", True)
            return True

        ########################################################################
        # Open The Comm Channel
        ########################################################################
        #

        # def get_enabled_link(_cfg):
        #     for link in _cfg.lb_conn_order:
        #         enabled = _cfg[link]["enabled"]
        #         if enabled:
        #             yield link
        #     return None

        # for link in self.cfg.lb_conn_order:

        bc = BotComm(self.cfg, self.log, "ethernet", 1, self.db)
        success = bc.getconn(0)

        br = botreports.LbConnItem('lb_ip', 'success')
        self.rpt_items.append(br)
        br.update_timestart()

        ########################################################################
        # Create the Float Message.
        ########################################################################
        # If there is a 'latest' Status Message from the current wake-up search,
        # pack the first one in the Result Set (that should be the latest Status
        # Record which should be sent whether it has associated Data Products or
        # not). Note that it is ordered by 'timestamp' to percolate the latest
        # Status Record to the top of the list. If a new Status Record is found,
        # it will be placed first in the uplink Message. Otherwise, skip this
        # process and continue on processing both new or previously-stored Data
        # Products.

        if self.cfg.tracking:
            self.log.track(0, "Create the Float Message.", True)

        if not haveNewStatus:
            if self.cfg.tracking:
                self.log.track(
                    1, "No NEW Status Messages Processed; Skip This Step.", True
                )
        else:
            if self.cfg.tracking:
                self.log.track(
                    1, "Find 'Latest' Active Status Record for Bot Message.", True
                )

            sql = "SELECT rowid, * FROM status WHERE rec_state = '0' ORDER BY timestamp DESC LIMIT 1"
            success, new_stat_results = self.db.getResults(2, sql, False)

            if success[0]:
                if not new_stat_results:
                    if self.cfg.tracking:
                        self.log.track(
                            2, "Can't Find Expected 'Latest' Active SR in DB.", True
                        )
                        self.log.track(2, "Ignore; Continue with Data Products.", True)
                else:
                    new_stat_rowid = new_stat_results[0][0]
                    new_stat_state = new_stat_results[0][1]
                    new_stat_stamp = float(new_stat_results[0][5])

                    if self.cfg.tracking:
                        self.log.track(2, "Found 'Latest' Active SR in Database.", True)
                        self.log.track(3, "rowid=[" + str(new_stat_rowid) + "]", True)
                        self.log.track(
                            3, "rec_state=[" + str(new_stat_state) + "]", True
                        )
                        self.log.track(3, "stamp=[" + str(new_stat_stamp) + "]", True)
                        self.log.track(
                            2,
                            "Add This 'Latest' Status Record to Uplink Message.",
                            True,
                        )

                    # success = sm.packstat(3, new_stat_results[0])
                    success = self.sm.encode_status_msg(
                        3, new_stat_results[0], self.dev_id_bytes, self.db
                    )
                    if success[0]:
                        br.update_msgsent(1)
                        br.update_statsent(1)
                        if self.cfg.tracking:
                            self.log.track(
                                1, "Latest Active SR Successfully Packed.", True
                            )
                            self.log.track(
                                1, "Update Status Record 'rec_state' to 'packed.'", True
                            )

                        sql = (
                                "UPDATE status SET rec_state = '1' WHERE rowid = '"
                                + str(new_stat_rowid)
                                + "'"
                        )
                        success = self.db.update(2, sql)
                        if success[0]:
                            if self.cfg.tracking:
                                self.log.track(
                                    2,
                                    "Latest Active SR 'status Updated (Packed).",
                                    True,
                                )
                                self.log.track(
                                    2, "Continue now with Data Products.", True
                                )
                        else:
                            if self.cfg.tracking:
                                self.log.track(2, "Well ... This is Awkward.", True)
                                self.log.track(
                                    2,
                                    "We May Eventually Repack This Same Status Record.",
                                    True,
                                )
                                self.log.track(
                                    2, "Ignore; Continue with Data Products.", True
                                )
                    else:
                        if self.cfg.tracking:
                            self.log.track(
                                2, "Can't Pack 'Latest' Active Status Record.", True
                            )
                            self.log.track(
                                2, "Ignore; Continue with Data Products.", True
                            )
            else:
                if self.cfg.tracking:
                    self.log.track(
                        2, "Can't Access 'Latest' Active Status Record from DB.", True
                    )
                    self.log.track(2, "Ignore; Continue with Data Products.", True)

        ########################################################################
        # Select Active/Unsent Meta Records for the Float Message.
        ########################################################################

        if self.cfg.tracking:
            self.log.track(
                0, "Select Top Active Data Products for Uplink Message.", True
            )

        have_active_dp = False

        sql = "SELECT rowid, * FROM data WHERE rec_state in (0,1) ORDER BY pipo DESC LIMIT 32"
        success, meta_rows = self.db.getResults(1, sql, False)

        if success[0]:
            if not meta_rows:
                if self.cfg.tracking:
                    self.log.track(
                        1, "NO Active Data Products in Database; Continue.", True
                    )
            else:
                have_active_dp = True
                if self.cfg.tracking:
                    self.log.track(1, "Active Data Products FOUND in Database.", True)
                    self.log.track(2, "Total: " + str(len(meta_rows)), True)
                    for row in meta_rows:
                        br.update_msgsent(1)
                        br.update_datasent(1)
                        self.log.track(
                            14,
                            "rowid=["
                            + str(row[0])
                            + "]  statid=["
                            # + str(row[3])
                            + str(row[2])
                            + "]  pipo=["
                            + str(row[18])
                            + "]",
                            True,
                        )
        else:
            if self.cfg.tracking:
                self.log.track(
                    1, "ERROR Finding Active Data Products in DB; Continue.", True
                )

        ########################################################################
        # Fill Balance of Uplink Message with Top Active Data Products.
        ########################################################################
        # Cycle through all active/unsent Data Products and add to the Float
        # message if they fit.

        if meta_rows:
            for row in meta_rows:
                metaID = str(row[0])
                statusFK = row[2]
                stat_state = 0
                stat_timestamp = 0
                if self.cfg.tracking:
                    self.log.track(
                        1, "Process Meta Data Product ID #" + str(metaID), True
                    )
                    self.log.track(2, "Get Associated Status Record.", True)
                    self.log.track(3, "Status FK: " + str(statusFK), True)

                # If not already packed in this Message, get it out of the 'status'
                # Table in DB. Then, pack this Status Record in the uplink Message
                # and save the Status Record's timestamp.
                # if not stat_sent_flag:
                sql = (
                        "SELECT rowid, * FROM status WHERE rowid = '" + str(statusFK) + "'"
                )
                success, assoc_statrec = self.db.getResults(3, sql, False)
                if success[0]:
                    if not assoc_statrec:
                        if self.cfg.tracking:
                            self.log.track(
                                3, "Can't Access Assoc Status Record in DB.", True
                            )
                            self.log.track(
                                3, "Likely a Previously-Sent Status Record.", True
                            )
                            self.log.track(
                                3, "Ignore; Continue with Next Data Product.", True
                            )
                        continue
                    else:
                        stat_state = assoc_statrec[0][1]
                        stat_timestamp = assoc_statrec[0][5]

                        if stat_state == 1:
                            if self.cfg.tracking:
                                self.log.track(
                                    3, "Assoc SR JUST PACKED into This Msg.", True
                                )
                                self.log.track(4, "State: " + str(stat_state), True)
                                self.log.track(4, "Stamp: " + str(stat_timestamp), True)
                        elif stat_state == 2:
                            if self.cfg.tracking:
                                self.log.track(
                                    3, "Assoc SR Previously Sent per DB.", True
                                )
                                self.log.track(4, "State: " + str(stat_state), True)
                                self.log.track(4, "Stamp: " + str(stat_timestamp), True)
                        else:
                            if self.cfg.tracking:
                                self.log.track(
                                    3, "Assoc SR FOUND in DB, but NOT Sent.", True
                                )
                                self.log.track(4, "State: " + str(stat_state), True)
                                self.log.track(4, "Stamp: " + str(stat_timestamp), True)
                                self.log.track(
                                    2, "PACK it into THIS Uplink Message.", True
                                )
                            success = self.sm.encode_status_msg(
                                3, assoc_statrec[0], self.dev_id_bytes, self.db
                            )
                            br.update_msgsent(1)
                            br.update_statsent(1)
                            if success[0]:
                                if self.cfg.tracking:
                                    self.log.track(
                                        3, "PACKED Assoc SR into This Message.", True
                                    )

                                sql = (
                                        "UPDATE status SET rec_state = '1' WHERE ROWID = '"
                                        + str(assoc_statrec[0][0])
                                        + "'"
                                )

                                if self.cfg.tracking:
                                    self.log.track(
                                        3, "Update Assoc SR 'state' to 'packed.'", True
                                    )

                                success = self.db.update(4, sql)
                                if success[0]:
                                    if self.cfg.tracking:
                                        self.log.track(
                                            3, "Assoc SR Status Updated (Packed).", True
                                        )
                                else:
                                    if self.cfg.tracking:
                                        self.log.track(
                                            3, "Well ... This is Awkward.", True
                                        )
                                        self.log.track(
                                            3,
                                            "We May Eventually Repack This Same SR.",
                                            True,
                                        )
                                        self.log.track(
                                            3,
                                            "Ignore; Continue with Next Data Product.",
                                            True,
                                        )
                                    continue
                else:
                    if self.cfg.tracking:
                        self.log.track(3, "Assoc SR NOT Available from DB.", True)
                        self.log.track(
                            3, "Ignore; Continue with Next Data Product.", True
                        )
                    continue

                # ---------------------------------------------------------------
                # Got Associated Status Record, so PACK this Data Product.
                # ---------------------------------------------------------------
                if self.cfg.tracking:
                    self.log.track(
                        2, "Pack This Data Product into Uplink Message.", True
                    )

                success = self.sm.encode_data_msg(
                    3, row, self.dev_id_bytes, self.db
                )  # stat_timestamp, next_index
                br.update_msgsent(1)
                br.update_datasent(1)
                if success[0]:
                    if self.cfg.tracking:
                        self.log.track(
                            3, "PACKED Data Product Record into Message.", True
                        )
                        self.log.track(
                            3, "Update Meta Record 'state' to 'packed.'", True
                        )

                        sql = (
                                "UPDATE data SET rec_state = '1' WHERE rowid = '"
                                + str(row[0])
                                + "'"
                        )
                        success = self.db.update(4, sql)
                        if not success[0]:
                            if self.cfg.tracking:
                                self.log.track(4, "Well ... This is Awkward.", True)
                                self.log.track(
                                    4,
                                    "Probably Come Back to Bite Us in the Ass.'",
                                    True,
                                )
                                self.log.track(
                                    4,
                                    "We May Eventually Repack This Same Meta Record.",
                                    True,
                                )
                else:
                    if self.cfg.tracking:
                        self.log.track(3, "Can't PACK Data Product Record.", True)
                        continue

                # send messages from msg_outgoing
                if self.msgs_outgoing:  # TODO: CHECK original not
                    if success[0]:
                        self.log.track(0, "Sending: ", True)
                        for item in self.msgs_outgoing:
                            send_success, cnc_msg = bc.send(1, item, 5, br)
                            if send_success[0]:
                                self.log.track(0, "send returned Success", True)
                                bcsuccess = 1  # Added as gap fix for no scuttle
                            else:
                                self.log.track(0, "send returned Not Success", True)
                                bcsuccess = 0  # Added as gap fix for no scuttle

                    else:
                        send_success = [False, None, None]
                        self.msgs_outgoing = None
                        self.log.track(0, "getconn returned Not Success", True)
                        bcsuccess = 0  # Added as gap fix for no scuttle
                else:
                    if self.cfg.tracking:
                        self.log.track(0, "NO Uplink Message to Send.", True)

        if self.cfg.tracking:
            self.log.track(1, "Final Message Complete.", True)
            #self.log.track(2, "Buf Len: " + str(len(str(self.sm.buf))), True)
            # TODO Check hex encoding
            # log.track(2, "Buf Msg: " + str(sm.buf).encode("hex"), True)
            # log.track(2, "Buf Msg: " + str(binascii.hexlify(str.encode(sm.buf))), True)

        ########################################################################
        # Get any DO general messages and encode for server
        ########################################################################

        gen_msg_dir = str(botdefs.nepi_home + "/lb/do-msg")

        for filename in pathlib.Path(gen_msg_dir).glob("*.json"):
            try:
                if self.cfg.tracking:
                    self.log.track(
                        0,
                        f"Found device general message to send to server [{filename}]",
                        True,
                    )
                with open(filename) as json_file:
                    data = json.load(json_file)
                if self.cfg.tracking:
                    self.log.track(0, f"File contains: {data}", True)
                ident = data.get("identifier", None)
                value = data.get("value", None)
                if ident is None or value is None:
                    self.log.track(
                        0,
                        f"missing 'identifier' or 'value' fields. Continuing with next message...",
                        True,
                    )
                    pathlib.Path(filename).unlink(missing_ok=True)
                    continue
                success = self.sm.encode_gen_msg(
                    0, ident, value, 1, self.dev_id_bytes, self.db
                )
                br.update_gensent(1)
            except Exception as e:
                if self.cfg.tracking:
                    self.log.track(
                        0,
                        f"Defective NepiBot Configuration file [{filename}]. Continuing with next message...",
                        True,
                    )
            pathlib.Path(filename).unlink(missing_ok=True)

        ########################################################################
        # Open Communications Port and Send the Messages.
        ########################################################################

        # def get_enabled_link(_cfg):
        #     for link in _cfg.lb_conn_order:
        #         enabled = _cfg[link]["enabled"]
        #         if enabled:
        #             yield link
        #     return None
        #
        # bc = BotComm(self.cfg, self.log, "ethernet", 1)
        # success = bc.getconn(0)

        # Receive messages from server
        recv_success = bc.receive(1, 1)
        if recv_success[0]:
            self.log.track(0, "receive returned Success", True)
            bcsuccess = 1
        else:
            recv_success = [False, None, None]

        # process incoming messages
        if len(self.msgs_incoming) == 0:
            self.log.track(
                0,
                f"No messages received from Server. Moving on to outgoing messages... ",
                True,
            )

        while len(self.msgs_incoming) > 0:
            self.log.track(
                0,
                f"Received {len(self.msgs_incoming)} messages from Server. Proceeding to process...",
                True,
            )

            #   decode each message individually
            cur_msg = self.msgs_incoming[0]
            operator.delitem(self.msgs_incoming, 0)
            self.log.track(
                1, f"Size Of Current Message in incoming queue: {len(cur_msg)}", True
            )
            (
                success,
                msg_routing,
                svr_comm_index,
                msg_type,
                msg_stack,
                orig_msg_fmt,
                dev_msg_fmt,
            ) = self.sm.decode_server_msg(0, cur_msg, self.dev_id_bytes)

            # if decoded message is empty, then move on to text message
            if not msg_stack:
                if self.cfg.tracking:
                    self.log.track(
                        1,
                        "Invalid message decoded. Message ignored. Continuing to next message",
                        True,
                    )
                continue
            # message for nepi bot

            # determine what to do with each message
            if msg_routing == 0:
                try:
                    if msg_type == "cfg_msg":
                        br.update_msgrecv(1)
                        br.update_cfgrecv(1)
                        for i in msg_stack.keys():
                            resetCfgValue(self.cfg, self.log, 2, str(i), msg_stack[i])
                    elif msg_type == "gen_msg":
                        br.update_msgrecv(1)
                        br.update_genrecv(1)
                        for i in msg_stack.items():
                            if i == "ping":
                                self.sm.encode_gen_msg(
                                    0,
                                    "ping",
                                    "NEPI-BOT is alive",
                                    0,
                                    self.dev_id_bytes,
                                    self.db,
                                )
                            elif i == "get_current_config":
                                self.sm.encode_gen_msg(
                                    0,
                                    "get_current_config",
                                    "Not Implemented",
                                    0,
                                    self.dev_id_bytes,
                                    self.db,
                                )
                            # TODO: change so that factory config is stored in database and then reinitialized
                            elif i == "reset_config":
                                self.sm.encode_gen_msg(
                                    0,
                                    "reset_config",
                                    "Not Implemented",
                                    0,
                                    self.dev_id_bytes,
                                    self.db,
                                )
                            elif i == "get_bot_version":
                                self.sm.encode_gen_msg(
                                    0,
                                    "get_bot_version",
                                    self.v_botmain,
                                    0,
                                    self.dev_id_bytes,
                                    self.db,
                                )
                            else:
                                self.sm.encode_gen_msg(
                                    0,
                                    i,
                                    "command unknown",
                                    0,
                                    self.dev_id_bytes,
                                    self.db,
                                )
                except Exception as e:
                    if self.cfg.tracking:
                        self.log.track(
                            0,
                            f"Problem processing gen/cfg message for nepibot '{e}'",
                            True,
                        )
                        self.log.track(
                            0, f"Original Message: {' '.join(orig_msg_fmt)}", True
                        )
                    pass

            # message for device

            elif msg_routing == 1:
                try:
                    if msg_type == "cfg_msg":
                        br.update_msgrecv(1)
                        br.update_cfgrecv(1)
                        fname = "/".join(
                            (
                                self.cfg.lb_cfg_dir_path,
                                f"/cfg_msg_{svr_comm_index:010d}.json",
                            )
                        )
                        with open(fname, "w") as f:
                            f.write(dev_msg_fmt)
                    elif msg_type == "gen_msg":
                        br.update_msgrecv(1)
                        br.update_genrecv(1)
                        fname = (
                                self.nepi_home
                                + f"/lb/dt-msg/gen_msg_{svr_comm_index:010d}.json"
                        )
                        with open(fname, "w") as f:
                            f.write(dev_msg_fmt)
                    else:
                        self.log.track(
                            0,
                            f"Message type '{msg_type}' unknown. Message skipped.",
                            True,
                        )
                except Exception as e:
                    if self.cfg.tracking:
                        self.log.track(
                            0,
                            f"Problem processing gen/cfg message for device '{e}'",
                            True,
                        )
                        self.log.track(
                            0, f"Original Message: {' '.join(orig_msg_fmt)}", True
                        )
            else:
                if self.cfg.tracking:
                    self.log.track(0, f"Unknown message routing '{msg_routing}'", True)
                    self.log.track(0, "Continuing to next message...", True)

        if len(self.msgs_outgoing) > 0:
            if success[0]:
                self.log.track(0, "Sending: ", True)
                for item in self.msgs_outgoing:
                    br.update_msgsent(1)
                    send_success, cnc_msg = bc.send(1, item, 5, br)
                    if send_success[0]:
                        self.log.track(0, "send returned Success", True)
                        bcsuccess = 1  # Added as gap fix for no scuttle
                    else:
                        self.log.track(0, "send returned Not Success", True)
                        bcsuccess = 0  # Added as gap fix for no scuttle

            else:
                send_success = [False, None, None]
                self.msgs_outgoing = None
                self.log.track(0, "getconn returned Not Success", True)
                bcsuccess = 0  # Added as gap fix for no scuttle
        else:
            if self.cfg.tracking:
                self.log.track(0, "NO Uplink Message to Send.", True)

        # Receive messages from server ... again
        recv_success = bc.receive(1, 1)
        if recv_success[0]:
            self.log.track(0, "receive returned Success", True)
            bcsuccess = 1
        else:
            recv_success = [False, None, None]

        # process incoming messages
        if len(self.msgs_incoming) == 0:
            self.log.track(
                0,
                f"No messages received from Server on second attempt. Going to cleanup... ",
                True,
            )
        else:
            self.log.track(
                0,
                f"Received {len(self.msgs_incoming)} messages from Server. Proceeding to process...",
                True,
            )
        while len(self.msgs_incoming) > 0:

            #   decode each message individually
            cur_msg = self.msgs_incoming[0]
            operator.delitem(self.msgs_incoming, 0)
            self.log.track(
                1, f"Size Of Current Message in incoming queue: {len(cur_msg)}", True
            )
            (
                success,
                msg_routing,
                svr_comm_index,
                msg_type,
                msg_stack,
                orig_msg_fmt,
                dev_msg_fmt,
            ) = self.sm.decode_server_msg(0, cur_msg, self.dev_id_bytes)

            if not success[0]:
                continue

            # determine what to do with each message

            # message for nepi bot

            if msg_routing == 0:
                try:
                    if msg_type == "cfg_msg":
                        br.update_msgrecv(1)
                        br.update_cfgrecv(1)
                        for i in msg_stack.keys():
                            resetCfgValue(self.cfg, self.log, 2, str(i), msg_stack[i])
                    elif msg_type == "gen_msg":
                        for i in msg_stack.items():
                            if i == "ping":
                                self.sm.encode_gen_msg(
                                    0, "ping", "alive", 0, self.dev_id_bytes, self.db
                                )
                            elif i == "get_current_config":
                                self.sm.encode_gen_msg(
                                    0,
                                    "get_current_config",
                                    "NI",
                                    0,
                                    self.dev_id_bytes,
                                    self.db,
                                )
                            elif i == "reset_config":
                                self.sm.encode_gen_msg(
                                    0,
                                    "reset_config",
                                    "config reset",
                                    0,
                                    self.dev_id_bytes,
                                    self.db,
                                )
                            elif i == "get_bot_version":
                                self.sm.encode_gen_msg(
                                    0,
                                    "get_bot_version",
                                    self.v_botmain,
                                    0,
                                    self.dev_id_bytes,
                                    self.db,
                                )
                            else:
                                self.sm.encode_gen_msg(
                                    0,
                                    i,
                                    "command unknown",
                                    0,
                                    self.dev_id_bytes,
                                    self.db,
                                )
                except Exception as e:
                    if self.cfg.tracking:
                        self.log.track(
                            0,
                            f"Problem processing gen/cfg message for nepibot '{e}'",
                            True,
                        )
                        self.log.track(
                            0, f"Original Message: {' '.join(orig_msg_fmt)}", True
                        )
                    pass

            # message for device

            elif msg_routing == 1:
                try:
                    if msg_type == "cfg_msg":
                        br.update_msgrecv(1)
                        br.update_cfgrecv(1)
                        
                        fname = (
                                botdefs.nepi_home
                                + self.cfg.lb_cfg_dir
                                + f"/cfg_msg_{svr_comm_index:010d}.json"
                        )
                        with open(fname, "w") as f:
                            f.write(dev_msg_fmt)
                    elif msg_type == "gen_msg":
                        br.update_msgrecv(1)
                        br.update_genrecv(1)
                        fname = (
                                botdefs.nepi_home
                                + f"/lb/dt-msg/gen_msg_{svr_comm_index:010d}.json"
                        )
                        with open(fname, "w") as f:
                            f.write(dev_msg_fmt)
                    else:
                        self.log.track(
                            0,
                            f"Message type '{msg_type}' unknown. Message skipped.",
                            True,
                        )
                except Exception as e:
                    if self.cfg.tracking:
                        self.log.track(
                            0,
                            f"Problem processing gen/cfg message for device '{e}'",
                            True,
                        )
                        self.log.track(
                            0, f"Original Message: {' '.join(orig_msg_fmt)}", True
                        )
            else:
                if self.cfg.tracking:
                    self.log.track(0, f"Unknown message routing '{msg_routing}'", True)
                    self.log.track(0, "Continuing to next message...", True)

        if not self.msgs_outgoing:
            if success[0]:
                self.log.track(0, "Sending: ", True)
                for item in self.msgs_outgoing:
                    br.update_msgsent(1)
                    send_success, cnc_msg = bc.send(1, item, 5, br)
                    if send_success[0]:
                        self.log.track(0, "send returned Success", True)
                        bcsuccess = 1  # Added as gap fix for no scuttle
                    else:
                        self.log.track(0, "send returned Not Success", True)
                        bcsuccess = 0  # Added as gap fix for no scuttle

            else:
                send_success = [False, None, None]
                self.msgs_outgoing = None
                self.log.track(0, "getconn returned Not Success", True)
                bcsuccess = 0  # Added as gap fix for no scuttle
        else:
            if self.cfg.tracking:
                self.log.track(0, "NO Uplink Message to Send.", True)

        success = bc.close(1)

        ########################################################################
        # Make sure any downlinked commands get processed.
        ########################################################################
        sdk_action = False
        # Botcomm indicates no cnc_msgs with either None or an empty list.
        # if msgs_incoming is None:
        #     msgs_incoming = list()
        # for msgnum in range(0, len(msgs_incoming)):
        #     if cfg.devclass == "generic":
        #         msg = msgs_incoming[msgnum]
        #         msg_pos = 0
        #         msg_len = len(msg)
        #         if cfg.tracking:
        #             log.track(1, "Evaluating C&C Message #" + str(msgnum), True)
        #             log.track(2, "msg_pos: [" + str(msg_pos) + "]", True)
        #             log.track(2, "msg_len: [" + str(msg_len) + "]", True)
        #     else:
        #         msg = msgs_incoming[msgnum]
        #         msg_pos = 0
        #         msg_len = len(msg)
        #
        #         if cfg.tracking:
        #             log.track(1, "Evaluating C&C Message #" + str(msgnum), True)
        #             log.track(2, "msg_pos: [" + str(msg_pos) + "]", True)
        #             log.track(2, "msg_len: [" + str(msg_len) + "]", True)
        #             # log.track(14, "msg_hex: [" + str(msg_hex) + "] <-- s/b double.", True)
        #     # -------------------------------------------------------------------
        #     # Loop Through the C&C Segments Until Message is Exhausted.
        #     # ------------------------------------------------------------------_
        #     # TODO Handle more than one message in packet
        #
        #     if cfg.devclass == "generic":
        #         servermsg = nepi_messaging_all_pb2.NEPIMsg()
        #         # servermsg_len = servermsg.ParseFromString(msg)
        #         new_msg_json = json_format.MessageToJson(
        #             servermsg, preserving_proto_field_name=False, indent=2
        #         )
        #         print(new_msg_json)

        ########################################################################
        # SDK C&C Downlink Message Notification.
        ########################################################################
        # if cfg.tracking:
        #     log.track(0, "SDK C&C Downlink Message Notification.", True)
        # if sdk_action:
        #     if cfg.tracking:
        #         log.track(1, "Messages Processed; Proceed.", True)
        #     if cfg.devclass == "float1":
        #         sdkproc = "/opt/numurus/ros/nepi-utilities/process-updates.sh"
        #         sdkwhat = "Live Float Mode"
        #     else:
        #         sdkproc = str(nepi_home) + "/src/tst/sdktest.sh"
        #         sdkwhat = "Local Non-Float Mode."
        #     try:
        #         if cfg.tracking:
        #             log.track(1, str(sdkwhat), True)
        #             log.track(1, "File: [" + str(sdkproc) + "]", True)
        #             log.track(1, "Setting 'devnull' device", True)
        #         devnull = open(os.devnull, "wb")
        #         if cfg.tracking:
        #             log.track(1, "Execute Popen w/nohup.", True)
        #         log.track(1, "Looking for sdkproc at: {}".format(str(sdkproc)), True)
        #         if os.path.isfile(str(sdkproc)):
        #             proc = Popen(
        #                 ["nohup", "/bin/sh", str(sdkproc)], stdout=devnull, stderr=devnull
        #             )
        #             rc = proc.wait()
        #             log.track(1, "{} returned {}".format(str(sdkproc), rc), True)
        #         else:
        #             log.track(1, os.listdir("/opt/numurus/ros/nepi-utilities/"), True)
        #             raise ValueError("Can't find SDK Notification Process.")
        #         if cfg.tracking:
        #             log.track(1, "DONE with SDK Notification.", True)
        #     except Exception as e:
        #         if cfg.tracking:
        #             log.track(2, "Errors(s) Executing Local Test App.", True)
        #             log.track(2, "ERROR: [" + str(e) + "]", True)
        #             log.track(2, "Continue.", True)
        # else:
        #     if cfg.tracking:
        #         log.track(1, "NO Messages Processed: Ignore SDK Notification.", True)
        #

        ########################################################################
        # Database Housekeeping.
        ########################################################################

        if self.cfg.tracking:
            self.log.track(0, "Perform DB Housekeeping.", True)

        if self.sm.len > 0:
            if send_success[0]:
                if self.cfg.tracking:
                    self.log.track(
                        1, "Update Bit-Packed Status Record(s) to 'sent' Status.", True
                    )

                sql = "UPDATE status SET rec_state = '2' WHERE rec_state = '1'"
                success = self.db.update(2, sql)
                if not success[0]:
                    if self.cfg.tracking:
                        self.log.track(2, "Well ... This is Awkward.", True)
                else:
                    if self.cfg.tracking:
                        self.log.track(2, "Done.", True)

                if self.cfg.tracking:
                    self.log.track(
                        1, "Update Bit-Packed Meta Record(s) to 'sent' Status.", True
                    )

                sql = "UPDATE data SET rec_state = '2' WHERE rec_state = '1'"
                success = self.db.update(2, sql)
                if not success[0]:
                    if self.cfg.tracking:
                        self.log.track(2, "Well ... This is Awkward.", True)
                else:
                    if self.cfg.tracking:
                        self.log.track(2, "Done.", True)

                if self.cfg.tracking:
                    self.log.track(1, "Update 'node' Table.", True)

                sql = "UPDATE node SET node_index = node_stage"
                success = self.db.update(2, sql)

                if not success[0]:
                    if self.cfg.tracking:
                        self.log.track(2, "Well ... This is Awkward.", True)
                else:
                    if self.cfg.tracking:
                        self.log.track(2, "Done.", True)
            else:
                if self.cfg.tracking:
                    self.log.track(
                        1,
                        "Reset Bit-Packed Status Record(s) to 'new/active' Status.",
                        True,
                    )

                sql = "UPDATE status SET rec_state = '0' WHERE rec_state = '1'"
                success = self.db.update(2, sql)
                if not success[0]:
                    if self.cfg.tracking:
                        self.log.track(2, "Well ... This is Awkward.", True)
                else:
                    if self.cfg.tracking:
                        self.log.track(2, "Done.", True)

                if self.cfg.tracking:
                    self.log.track(
                        1,
                        "Reset Bit-Packed Meta Record(s) to 'new/active' Status.",
                        True,
                    )

                sql = "UPDATE data SET rec_state = '0' WHERE rec_state = '1'"
                success = self.db.update(2, sql)
                if not success[0]:
                    if self.cfg.tracking:
                        self.log.track(2, "Well ... This is Awkward.", True)
                else:
                    if self.cfg.tracking:
                        self.log.track(2, "Done.", True)

                if self.cfg.tracking:
                    self.log.track(1, "Update 'node' Table.", True)

                sql = "UPDATE node SET node_stage = node_index"
                success = self.db.update(2, sql)

                if not success[0]:
                    if self.cfg.tracking:
                        self.log.track(2, "Well ... This is Awkward.", True)
                else:
                    if self.cfg.tracking:
                        self.log.track(2, "Done.", True)
        else:
            if self.cfg.tracking:
                self.log.track(1, "NO Message = NO Housekeeping to be Done.", True)

        br.update_timestop()
        return 0, self.rpt_items
        # save botcomm_index to db and close db
        #success = self.db.close(0)
