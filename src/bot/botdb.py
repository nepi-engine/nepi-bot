########################################################################
##
# Module: botdb.py
# ----------------
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
import datetime
import os
import sqlite3
from dateutil.parser import parse
from regex import regex
import bothelp

v_botdb = "bot71-20200601"
sqlite3_db_current_ver = 15

# -------------------------------------------------------------------


def dict_factory(cursor, row):
    # -------------------------------------------------------------------
    # This code will help extract a group of dictionaries based on
    # the SELECT statement from the SQLite database and dump it into
    # JSON for use.
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


########################################################################
# The Nepibot Database Class
########################################################################


class BotDB(object):
    def __init__(self, _cfg, _log, _lev):
        self.dbc = None
        self.dbr = None
        self.cfg = _cfg
        self.log = _log
        self.bot_comm_index = 0
        if self.cfg.tracking:
            self.log.track(_lev, "Created DB Class Object: ", True)
            self.log.track(_lev + 13, "^dbc: " + str(self.dbc), True)
            self.log.track(_lev + 13, "^dbr: " + str(self.dbr), True)
            self.log.track(_lev + 13, "^log: " + str(self.log), True)
            self.log.track(_lev + 13, "^cfg: " + str(self.cfg), True)
            self.log.track(_lev + 13, "_lev: " + str(_lev), True)

    def getconn(self, _lev):
        # Establish a connection. SQLite either creates or opens a file
        # (in this case, the 'dbfile' passed to the SQLite class method
        # above).  If the file is there, we assume that it is a legit
        # Nepibot Database.  If not there, the DB requires instantiation.
        if self.cfg.tracking:
            self.log.track(_lev, "Entering DB 'getconn()' Method.", True)
            self.log.track(_lev + 1, "Establish DB Connection.", True)

        if os.path.isfile(self.cfg.db_file):
            if self.cfg.tracking:
                self.log.track(
                    _lev + 2, "DB File Found: " + str(self.cfg.db_file), True
                )
        else:
            if self.cfg.tracking:
                self.log.track(
                    _lev + 2, "DB File NOT Found: " + str(self.cfg.db_file), True
                )
            success = self.reset(_lev + 2)
            if not success[0]:
                return success, None

        try:
            self.dbc = sqlite3.connect(self.cfg.db_file)
            if self.cfg.tracking:
                self.log.track(_lev + 1, "DB Connection Established.", True)
            return [True, None, None], self.dbc
        except Exception as e:
            enum = "DB001"
            emsg = "getconn(): [" + str(e) + "]"
            if self.cfg.tracking:
                self.log.errtrack(str(enum), str(emsg))
            return [False, str(enum), str(emsg)], None

    # -------------------------------------------------------------------

    def get_botcomm_index(self):
        if self.cfg.tracking:
            self.log.track(1, "Getting value of bot_comm_index from DB.", True)
        _sql = (
            "UPDATE counters SET bot_comm_index = bot_comm_index + 1 WHERE ROWID = 1;"
        )
        _sql2 = "SELECT bot_comm_index FROM counters WHERE ROWID = 1;"
        cursor = self.dbc.cursor()
        cursor.execute(str(_sql))
        cursor.execute(str(_sql2))
        results = cursor.fetchone()
        self.dbc.commit()
        if self.cfg.tracking:
            self.log.track(1 + 1, "SQL Executed.", True)
        self.bot_comm_index = int(results[0])
        if self.cfg.tracking:
            self.log.track(1, f"bot_comm_index set to {self.bot_comm_index}.", True)
        return self.bot_comm_index

    def pushstat(self, _lev, _statjson):
        # INSERT a 'status' record into the Float DB.  This Module is
        # designed to insert the exact number of columns by picking
        # apart the 'statjson' JSON-formatted 'Status' record as
        # retrieved from the Data Folder and subjected to a json.load.
        if self.cfg.tracking:
            self.log.track(_lev, "Entering DB 'pushstat()' Module.", True)
            self.log.track(_lev + 1, "_lev: " + str(_lev), True)
            self.log.track(_lev + 13, "_JSON: " + str(_statjson), True)
            self.log.track(_lev + 1, "Format SQL INSERT Statement.", True)

        # ---------------------------------------------------------------
        # Create the SQL statement for inserting the Status Record into
        # the 'float.db' database.  This includes a combination of JSON
        # values from the SDK's Status File in the Data Folder PLUS a
        # handfull of seeded control values for columns (fields) that
        # will be used later.
        try:
            # convert timestamp from iso string to float
            tempdt = parse(_statjson["timestamp"])
            tempts = tempdt.timestamp()

            data_tuple = (
                float(tempts),
                int(_statjson.get("navsat_fix_time_offset", 0)),
                float(_statjson.get("latitude", 0.0)),
                float(_statjson.get("longitude", 0.0)),
                int(_statjson.get("heading", 0)),
                int(_statjson.get("heading_ref", 0)),
                int(_statjson.get("roll_angle", 0)),
                int(_statjson.get("pitch_angle", 0)),
                int(_statjson.get("temperature", 0)),
                int(_statjson.get("power_state", 0)),
                bytes(_statjson.get("device_status", "")),
            )
            sql_statement = """\
            INSERT INTO status \
            VALUES (0,0,0,0,?,?,?,?,?,?,?,?,?,?,?)\
            """
        except Exception as e:
            enum = "DB002"
            emsg = "pushstat(): [" + str(e) + "]"
            if self.cfg.tracking:
                self.log.errtrack(str(enum), str(emsg))
            return [False, str(enum), str(emsg)], None

        # ---------------------------------------------------------------
        # Perform the actual INSERT and 'commit()' the new record to the
        # 'float.db' database.
        if self.cfg.tracking:
            self.log.track(_lev + 1, "INSERT Status Record into DB.", True)
            self.log.track(_lev + 14, "SQL: " + str(sql_statement), True)
            # self.log.track(_lev + 14, "DATA: " + str(data_tuple, sep="|"))

        try:
            cursor = self.dbc.cursor()
            cursor.execute(sql_statement, data_tuple)
            # cursor.execute(str(sql))
            lastrowid = cursor.lastrowid
            if self.cfg.tracking:
                self.log.track(_lev + 14, "Successful.", True)
                self.log.track(_lev + 14, "Row ID: " + str(lastrowid), True)
            self.dbc.commit()
        except Exception as e:
            enum = "DB003"
            emsg = "pushstat(): [" + str(e) + "]"
            if self.cfg.tracking:
                self.log.errtrack(str(enum), str(emsg))
            return [False, str(enum), str(emsg)], None

        return [True, None, None], lastrowid

    # -------------------------------------------------------------------
    # INSERT a 'Data' record into the Float DB. This Module is designed
    # to insert the exact number of columns by picking apart the
    # 'datajson' JSON-formatted 'Meta' record as retrieved from the
    # Data Folder and subjected to a json.load.  In addition, the
    # mandatory "standard" and optional "change" files are included.
    # def pushmeta(self, _lev, _datajson, _state, _status, _trigger, _numerator, _stdsize, _chgsize, _norm, _pipo, _metafile, _stdfile, _chgfile):
    def pushdata(
        self, _lev, _datajson, _info, _status, _trigger, _metafile, _status_rowid
    ):
        if self.cfg.logging:
            self.log.track(_lev, "Entering DB 'pushdata()' Module.", True)
            self.log.track(_lev + 1, "Log Level:    " + str(_lev), True)
            self.log.track(_lev + 13, "Data JSON:    " + str(_datajson), True)
            self.log.track(_lev + 1, "State:        " + str(_info[3]), True)
            self.log.track(_lev + 1, "Status ID:    " + str(_status_rowid), True)
            self.log.track(_lev + 1, "Trigger:      " + str(_trigger), True)
            self.log.track(_lev + 1, "Numerator:    " + str(_info[0]), True)
            self.log.track(_lev + 1, "Std Size:     " + str(_info[1]), True)
            self.log.track(_lev + 1, "Chg Size:     " + str(_info[2]), True)
            self.log.track(_lev + 1, "Normalize:    " + str(_info[4]), True)
            self.log.track(_lev + 1, "PIPO Rating:  " + str(_info[5]), True)
            self.log.track(_lev + 1, "MetaFile:     " + str(_metafile), True)
            self.log.track(_lev + 1, "StdFile:      " + str(_info[6]), True)
            self.log.track(_lev + 1, "ChgFile:      " + str(_info[7]), True)
            self.log.track(_lev + 1, "Chg Eligible: " + str(_info[3]), True)
            self.log.track(_lev + 13, "StdJSON:      " + str(_info[8]), True)
            self.log.track(_lev + 13, "ChgJSON:      " + str(_info[9]), True)
            self.log.track(_lev + 1, "Format SQL INSERT Statement.", True)

        chg_channels = 0
        chg_dtype = ""
        chg_deltas = ""

        try:
            if _info[9]:
                chg_channels = int(_info[9]["channel_count"])
                chg_dtype = str(_info[9]["dt"])
                chg_deltas = str(_info[9]["deltas"])
        except Exception as e:
            enum = "DB104"
            emsg = "pushdata(): [" + str(e) + "]"
            if self.cfg.tracking:
                self.log.errtrack(str(enum), str(emsg))
            return [False, str(enum), str(emsg)], None

        # ---------------------------------------------------------------
        # Create the SQL statement for inserting the Data Record into
        # the 'float.db' database.  This includes a combination of JSON
        # values from the SDK's Meta File in the Data Folder PLUS a
        # handfull of seeded control values for columns (fields) that
        # will be used later and specific values passed to this function
        # as arguments.

        try:
            try:
                filepath = regex.match(r".*/", _metafile)
                filename = filepath.group() + _datajson.get("data_file", "")
                with open(filename, mode="rb") as file:
                    filecontent = file.read()
                payload_data = _datajson.get("data_file")
                payload_fname, payload_fname_ext = os.path.splitext(payload_data)
            except Exception as e:
                payload_fname_ext = ""
                filecontent = b""

            data_tuple = (
                _status_rowid,
                float(_datajson.get("timestamp", 0.0)),
                str(_datajson.get("type", "")),
                int(_datajson.get("instance", 0)),
                int(_datajson.get("date_time_offset", 0)),
                int(_datajson.get("latitude_offset", 0)),
                int(_datajson.get("longitude_offset", 0)),
                int(_datajson.get("heading_offset", 0)),
                int(_datajson.get("roll_offset", 0)),
                int(_datajson.get("pitch_offset", 0)),
                str(_datajson.get("data_file", "")),
                str(payload_fname_ext),
                bytes(filecontent),
                float(_info[0]),
                int(0),
                float(_info[5]),
                float(1.0),
                float(_datajson.get("quality_score", 1.0)),
                float(_datajson.get("type_score", 1.0)),
                float(_datajson.get("event_score", 1.0)),
                str(""),
                str(""),
                int(0),
            )

            sql_statement = """\
            INSERT INTO data \
            VALUES (0,?,0,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""

        except Exception as e:
            enum = "DB004"
            emsg = "pushdata(): [" + str(e) + "]"
            if self.cfg.tracking:
                self.log.errtrack(str(enum), str(emsg))
            return [False, str(enum), str(emsg)], None

        # ---------------------------------------------------------------
        # Perform the actual INSERT and 'commit()' the new 'meta' record
        # to the 'float.db' database.
        if self.cfg.tracking:
            # self.log.track(_lev + 13, "SQL: " + str(sql), True)
            self.log.track(_lev, "Perform the 'Data' Record INSERT.", True)

        try:
            cursor = self.dbc.cursor()
            cursor.execute(sql_statement, data_tuple)
            lastrowid = cursor.lastrowid
            if self.cfg.tracking:
                self.log.track(_lev + 1, "Data Row ID: " + str(lastrowid), True)
            self.dbc.commit()
        except Exception as e:
            enum = "DB005"
            emsg = "pushdata(): [" + str(e) + "]"
            if self.cfg.tracking:
                self.log.errtrack(str(enum), str(emsg))
            return [False, str(enum), str(emsg)], None

        return [True, None, None], lastrowid

    # -------------------------------------------------------------------
    def getResults(self, _lev, _sql, _jflag):
        # -------------------------------------------------------------------
        if self.cfg.tracking:
            self.log.track(_lev, "Entering DB getResults() Module.", True)
            self.log.track(_lev + 1, "_lev:   " + str(_lev), True)
            self.log.track(_lev + 13, "_sql:   " + str(_sql), True)
            self.log.track(_lev + 1, "_jflag: " + str(_jflag), True)

        try:
            if _jflag:
                rowfac = self.dbc.row_factory
                self.dbc.row_factory = dict_factory
                if self.cfg.tracking:
                    self.log.track(_lev + 1, "Changed Row Factory.", True)

            cursor = self.dbc.cursor()
            if self.cfg.tracking:
                self.log.track(_lev + 1, "Got Cursor.", True)
            cursor.execute(str(_sql))
            if self.cfg.tracking:
                self.log.track(_lev + 1, "SQL Executed.", True)
            results = cursor.fetchall()
            if self.cfg.tracking:
                self.log.track(_lev + 1, "Fetched Rows.", True)

            if _jflag:
                self.dbc.row_factory = rowfac
                if self.cfg.tracking:
                    self.log.track(_lev + 1, "Changed Row Factory Back.", True)
        except Exception as e:
            enum = "DB006"
            emsg = "getResults(): [" + str(e) + "]"
            if self.cfg.tracking:
                self.log.errtrack(str(enum), str(emsg))
            return [False, str(enum), str(emsg)], None

        if self.cfg.tracking:
            self.log.track(_lev + 1, "Returning Result Set.", True)
            self.log.track(_lev + 13, "Results: " + str(results), True)

        return [True, None, None], results

    # -------------------------------------------------------------------
    def update(self, _lev, _sql):
        # -------------------------------------------------------------------
        if self.cfg.tracking:
            self.log.track(_lev, "Entering DB update() Module.", True)
            self.log.track(_lev + 13, "_lev: " + str(_lev), True)
            self.log.track(_lev + 13, "_sql: " + str(_sql), True)

        try:
            cursor = self.dbc.cursor()
            if self.cfg.tracking:
                self.log.track(_lev + 13, "Got Cursor.", True)
            cursor.execute(str(_sql))
            if self.cfg.tracking:
                self.log.track(_lev + 13, "SQL Executed.", True)
            self.dbc.commit()
            if self.cfg.tracking:
                self.log.track(_lev + 13, "Update Committed.", True)
        except Exception as e:
            enum = "DB007"
            emsg = "update(): [" + str(e) + "]"
            if self.cfg.tracking:
                self.log.errtrack(str(enum), str(emsg))
            return [False, str(enum), str(emsg)]

        return [True, None, None]

    # -------------------------------------------------------------------
    def reset(self, _lev):
        # -------------------------------------------------------------------
        # WARNING!  A call to this method WILL completely destroy and, then,
        # re-instantiate the ENTIRE Float DB.  This is used primarily during
        # development and testing, but can also be activated if a 'command
        # and control' message is sent from "the Cloud" during deployment
        # (e.g., when a complete Float reset is deemed necessary).
        if self.cfg.tracking:
            self.log.track(_lev, "Entering DB reset() Module.", True)
            self.log.track(_lev + 1, "_lev: " + str(_lev), True)

        try:
            # If the Float Database file exists, delete it.  If we can't
            # get rid of it, this App is dead in the water, at least for
            # now. In later revisions, we can create an memory-resident
            # DB so we can limp along sans Data Product retention (after
            # notifying "the Cloud" of the condition).
            if os.path.isfile(self.cfg.db_file):
                if self.cfg.tracking:
                    self.log.track(_lev + 1, "DB File Exists; Remove it.", True)
                try:
                    os.remove(self.cfg.db_file)
                    self.log.track(_lev + 2, "Removed DB File.", True)
                except Exception as e:
                    enum = "DB101"
                    emsg = "reset(): [" + str(e) + "]"
                    if self.cfg.tracking:
                        self.log.errtrack(str(enum), str(emsg))
                    return [False, str(enum), str(emsg)]
            else:
                if self.cfg.tracking:
                    self.log.track(_lev + 1, "DB File does not exist; Continue.", True)

            # Create the 'Database Directory' in case there wasn't one
            # to begin with.  This should never happen and requires
            # that the 'NEPI Home' directory is writable; if not, this
            # attempt will fail, rendering the entire DB functionality
            # moot.  The test for existence eliminates the possibilty
            # of os.makeidrs() failure due to the directory already
            # existing - a condition which is benign.
            dir = os.path.dirname(self.cfg.db_file)
            if self.cfg.tracking:
                self.log.track(_lev + 1, "Check DB Directory.", True)
                self.log.track(_lev + 2, "Dir: " + str(dir), True)

            if not os.path.isdir(str(dir)):
                if self.cfg.tracking:
                    self.log.track(_lev + 2, "DB Directory doesn't exist.", True)
                try:
                    os.makedirs(str(dir))
                    if self.cfg.tracking:
                        self.log.track(_lev + 2, "Created Dir: " + str(dir), True)
                except Exception as e:
                    enum = "DB102"
                    emsg = "reset(): [" + str(e) + "]"
                    if self.cfg.tracking:
                        self.log.errtrack(str(enum), str(emsg))
                    return [False, str(enum), str(emsg)]
            else:
                if self.cfg.tracking:
                    self.log.track(_lev + 2, "DB Directory Exists.", True)

            # -----------------------------------------------------------
            # Open a first connection with the DB (creates the DB file).
            if self.cfg.tracking:
                self.log.track(_lev + 1, "Connect with New Database.", True)

            try:
                self.dbr = sqlite3.connect(self.cfg.db_file)
            except Exception as e:
                enum = "DB103"
                emsg = "reset(): [" + str(e) + "]"
                if self.cfg.tracking:
                    self.log.errtrack(str(enum), str(emsg))
                return [False, str(enum), str(emsg)]

            # -----------------------------------------------------------
            # Get a cursor object for upcoming Table and Index creation
            # activity.
            if self.cfg.tracking:
                self.log.track(_lev + 1, "Get the DB Cursor.", True)

            try:
                cursor = self.dbr.cursor()
            except Exception as e:
                enum = "DB104"
                emsg = "reset(): [" + str(e) + "]"
                if self.cfg.tracking:
                    self.log.errtrack(str(enum), str(emsg))
                return [False, str(enum), str(emsg)]

            # -----------------------------------------------------------
            # Create the 'adapters' Table.
            # -----------------------------------------------------------

            if self.cfg.tracking:
                self.log.track(_lev + 1, "Create the 'adapters' Table.", True)

            try:
                cursor.execute(
                    """CREATE TABLE adapters (
                    name TEXT,
                    enabled INTEGER,
                    type TEXT,
                    host TEXT,
                    port TEXT,
                    baud INTEGER,
                    tout INTEGER,
                    open_attm INTEGER,
                    open_tout INTEGER,
                    protocol INTEGER,
                    max_msg_size INTEGER,
                    packet_size INTEGER,
                    pipo_scor_wt REAL,
                    pipo_qual_wt REAL,
                    pipo_size_wt REAL,
                    pipo_time_wt REAL,
                    pipo_trig_wt REAL,
                    purge_rating REAL
                    )
                    """
                )
            except Exception as e:
                enum = "DB105"
                emsg = "reset(): [" + str(e) + "]"
                if self.cfg.tracking:
                    self.log.errtrack(str(enum), str(emsg))
                return [False, str(enum), str(emsg)]

            # -----------------------------------------------------------
            # Create the 'sys_config' Table.
            # -----------------------------------------------------------

            if self.cfg.tracking:
                self.log.track(_lev + 1, "Create the 'sys_config' Table.", True)

            try:
                cursor.execute(
                    """CREATE TABLE sys_config (
                    type INTEGER NOT NULL,
                    devclass TEXT,
                    logging INTEGER,
                    locking INTEGER,
                    comms INTEGER,
                    fs_pct_used_warning INTEGER,
                    sys_status_file TEXT,
                    lb_data_dir TEXT,
                    lb_cfg_dir TEXT,
                    lb_req_dir TEXT,
                    lb_resp_dir TEXT,
                    hb_data_dir TEXT,
                    data_zlib INTEGER,
                    data_msgpack INTEGER,
                    db_dir TEXT,
                    db_name TEXT,
                    db_deletes INTEGER,
                    log_dir TEXT,
                    log_clear INTEGER,
                    bot_main_log_name INTEGER,
                    lb_conn_log_name TEXT,
                    conn_log INTEGER,
                    hb_conn_log_name TEXT,
                    packet_log INTEGER,
                    packet_log_name TEXT,
                    pipo_scor_wt REAL,
                    pipo_qual_wt REAL,
                    pipo_size_wt REAL,
                    pipo_time_wt REAL,
                    pipo_trig_wt REAL,
                    purge_rating REAL,
                    lb_encrypted INTEGER,
                    lb_conn_order TEXT,
                    hb_conn_order INTEGER
                    )
                    """
                )
            except Exception as e:
                enum = "DB106"
                emsg = "reset(): [" + str(e) + "]"
                if self.cfg.tracking:
                    self.log.errtrack(str(enum), str(emsg))
                return [False, str(enum), str(emsg)]

            # -----------------------------------------------------------
            # Create the 'config' Table.
            # -----------------------------------------------------------

            if self.cfg.tracking:
                self.log.track(_lev + 1, "Create the 'config' Table.", True)

            try:
                cursor.execute(
                    """CREATE TABLE "config" (
                    "rec_state"	INTEGER,
                    "timestamp"	REAL,
                    "comm_index" INTEGER,
                    "subsystem"	INTEGER,
                    "identifier" TEXT,
                    "identifier_dt"	INTEGER,
                    "payload" BLOB,
                    "payload_dt" INTEGER
                    )
                    """
                )
            except Exception as e:
                enum = "DB107"
                emsg = "reset(): [" + str(e) + "]"
                if self.cfg.tracking:
                    self.log.errtrack(str(enum), str(emsg))
                return [False, str(enum), str(emsg)]

            # -----------------------------------------------------------
            # Create the 'counters' Table.
            # -----------------------------------------------------------

            if self.cfg.tracking:
                self.log.track(_lev + 1, "Create the 'counters' Table.", True)

            try:
                cursor.execute(
                    """CREATE TABLE counters (
                    bot_comm_index INTEGER
                    )
                    """
                )
            except Exception as e:
                enum = "DB107"
                emsg = "reset(): [" + str(e) + "]"
                if self.cfg.tracking:
                    self.log.errtrack(str(enum), str(emsg))
                return [False, str(enum), str(emsg)]

            # -----------------------------------------------------------
            # Instantiate the 'counters' Table.
            # -----------------------------------------------------------

            if self.cfg.tracking:
                self.log.track(_lev + 1, "Instantiate the 'counters' Table.", True)

            try:
                cursor.execute("INSERT INTO counters VALUES (1);")
            except Exception as e:
                enum = "DB107"
                emsg = "reset(): [" + str(e) + "]"
                if self.cfg.tracking:
                    self.log.errtrack(str(enum), str(emsg))
                return [False, str(enum), str(emsg)]

            # -----------------------------------------------------------
            # Create the 'data' Table.
            # -----------------------------------------------------------

            if self.cfg.tracking:
                self.log.track(_lev + 1, "Create the 'data' Table.", True)

            try:
                cursor.execute(
                    """CREATE TABLE "data" (
                    "rec_state"	INTEGER,
                    "sys_status_ref_id"	INTEGER,
                    "comm_index"	INTEGER,
                    "timestamp" REAL,
                    "type"	TEXT,
                    "instance"	INTEGER,
                    "date_time_offset"	INTEGER,
                    "latitude_offset"	INTEGER,
                    "longitude_offset"	INTEGER,
                    "heading_offset"	INTEGER,
                    "roll_offset"	INTEGER,
                    "pitch_offset"	INTEGER,
                    "payload_fname"	TEXT,
                    "payload_fname_ext"	TEXT,
                    "payload"	BLOB,
                    "numerator"	REAL,
                    "trigger"	INTEGER,
                    "pipo"	REAL,
                    "quality"	REAL,
                    "quality_score" REAL,
                    "type_score" REAL,
                    "event_score" REAL,
                    "metafile"	TEXT,
                    "metafile_ext"	TEXT,
                    "chg_eligible"	INTEGER
                    )
                    """
                )
            except Exception as e:
                enum = "DB108"
                emsg = "reset(): [" + str(e) + "]"
                if self.cfg.tracking:
                    self.log.errtrack(str(enum), str(emsg))
                return [False, str(enum), str(emsg)]

            # -----------------------------------------------------------
            # Create the 'general' Table.
            # -----------------------------------------------------------

            if self.cfg.tracking:
                self.log.track(_lev + 1, "Create the 'general' Table.", True)

            try:
                cursor.execute(
                    """CREATE TABLE "general" (
                    "rec_state"	INTEGER,
                    "timestamp"	REAL,
                    "comm_index"	INTEGER,
                    "subsystem"	INTEGER,
                    "identifier"	TEXT,
                    "identifier_dt"	INTEGER,
                    "payload"	BLOB,
                    "payload_dt"	INTEGER
                    )
                    """
                )
            except Exception as e:
                enum = "DB109"
                emsg = "reset(): [" + str(e) + "]"
                if self.cfg.tracking:
                    self.log.errtrack(str(enum), str(emsg))
                return [False, str(enum), str(emsg)]

            # -----------------------------------------------------------
            # Create the 'status' Table.
            # -----------------------------------------------------------

            if self.cfg.tracking:
                self.log.track(_lev + 1, "Create the 'status' Table.", True)

            try:
                cursor.execute(
                    """CREATE TABLE status (
                    rec_state INTEGER,
                    sys_status_id INTEGER,
                    comm_index INTEGER,
                    meta_state INTEGER,
                    timestamp REAL,
                    navsat_fix_time_offset INTEGER,
                    latitude REAL,
                    longitude REAL,
                    heading INTEGER,
                    heading_true_north INTEGER,
                    roll_angle INTEGER,
                    pitch_angle INTEGER,
                    temperature INTEGER,
                    power_state INTEGER,
                    device_status BLOB
                    )
                    """
                )
            except Exception as e:
                enum = "DB110"
                emsg = "reset(): [" + str(e) + "]"
                if self.cfg.tracking:
                    self.log.errtrack(str(enum), str(emsg))
                return [False, str(enum), str(emsg)]

            # -----------------------------------------------------------
            # Create the 'node' Table.
            # -----------------------------------------------------------
            if self.cfg.tracking:
                self.log.track(_lev + 1, "Create the 'node' Table.", True)

            try:
                cursor.execute(
                    """CREATE TABLE node
                    (node_type TEXT,
                    node_code TINYINT,
                    node_instance TINYINT,
                    node_index TINYINT,
                    node_stage TINYINT)
                    """
                )
            except Exception as e:
                enum = "DB107"
                emsg = "reset(): [" + str(e) + "]"
                if self.cfg.tracking:
                    self.log.errtrack(str(enum), str(emsg))
                return [False, str(enum), str(emsg)]

            # -----------------------------------------------------------
            # Instantiate the 'node' Table.
            # -----------------------------------------------------------
            cmds = [
                "INSERT INTO node VALUES ('bat', 1, 0, 0, 0);",
                "INSERT INTO node VALUES ('thr', 2, 0, 0, 0);",
                "INSERT INTO node VALUES ('imu', 3, 0, 0, 0);",
                "INSERT INTO node VALUES ('gps', 5, 0, 0, 0);",
                "INSERT INTO node VALUES ('cam', 9, 0, 0, 0);",
                "INSERT INTO node VALUES ('esr', 10, 0, 0, 0);",
                "INSERT INTO node VALUES ('esp', 11, 0, 0, 0);",
                "INSERT INTO node VALUES ('ebi', 12, 0, 0, 0);",
                "INSERT INTO node VALUES ('ech', 13, 0, 0, 0);",
                "INSERT INTO node VALUES ('erd', 14, 0, 0, 0);",
                "INSERT INTO node VALUES ('epr', 15, 0, 0, 0);",
                "INSERT INTO node VALUES ('epc', 16, 0, 0, 0);",
                "INSERT INTO node VALUES ('ctp', 17, 0, 0, 0);",
                "INSERT INTO node VALUES ('crs', 18, 0, 0, 0);",
                "INSERT INTO node VALUES ('cbd', 19, 0, 0, 0);",
                "INSERT INTO node VALUES ('cav', 20, 0, 0, 0);",
                "INSERT INTO node VALUES ('cca', 21, 0, 0, 0);",
                "INSERT INTO node VALUES ('crb', 22, 0, 0, 0);",
                "INSERT INTO node VALUES ('cgs', 23, 0, 0, 0);",
                "INSERT INTO node VALUES ('cgp', 24, 0, 0, 0);",
                "INSERT INTO node VALUES ('ugp', 25, 0, 0, 0);",
                "INSERT INTO node VALUES ('nbd', 26, 0, 0, 0);",
                "INSERT INTO node VALUES ('nlg', 27, 0, 0, 0);",
                "INSERT INTO node VALUES ('fth', 28, 0, 0, 0);",
                "INSERT INTO node VALUES ('fcf', 29, 0, 0, 0);",
                "INSERT INTO node VALUES ('ftm', 30, 0, 0, 0);",
                "INSERT INTO node VALUES ('fwn', 31, 0, 0, 0);",
                "INSERT INTO node VALUES ('frs', 32, 0, 0, 0);",
                "INSERT INTO node VALUES ('fbb', 33, 0, 0, 0);",
                "INSERT INTO node VALUES ('fsg', 34, 0, 0, 0);",
                "INSERT INTO node VALUES ('fpk', 35, 0, 0, 0);",
                "INSERT INTO node VALUES ('fof', 36, 0, 0, 0);",
                "INSERT INTO node VALUES ('fdn', 37, 0, 0, 0);",
                "INSERT INTO node VALUES ('rch', 38, 0, 0, 0);",
                "INSERT INTO node VALUES ('cla', 39, 0, 0, 0);",
                "INSERT INTO node VALUES ('sin', 40, 0, 0, 0);",
                "INSERT INTO node VALUES ('sbd', 42, 0, 0, 0);",
            ]

            if self.cfg.tracking:
                self.log.track(3, "Instantiated the 'node' Table.", True)

            for sql in cmds:
                try:
                    cursor.execute(str(sql))
                    self.dbr.commit()
                    if self.cfg.tracking:
                        self.log.track(4, str(sql), True)
                except Exception as e:
                    enum = "DB116"
                    emsg = "reset(): [" + str(e) + "]"
                    if self.cfg.tracking:
                        self.log.errtrack(str(enum), str(emsg))
                    return [False, str(enum), str(emsg)], None

            # -----------------------------------------------------------
            # Close the Database.
            # -----------------------------------------------------------

            success = self.close(_lev + 1)

            return success

        except Exception as e:
            if self.cfg.tracking:
                enum = "DB108"
                emsg = "reset(): [" + str(e) + "]"
                if self.cfg.tracking:
                    self.log.errtrack(str(enum), str(emsg))
                return [False, str(enum), str(emsg)]

    def close(self, _lev):
        # -------------------------------------------------------------------
        if self.cfg.tracking:
            self.log.track(_lev, "Close the DB Connection.", True)
            self.log.track(_lev + 13, "_lev: " + str(_lev), True)

        rtrn = True
        enum = None
        emsg = None

        if self.dbr:
            try:
                self.dbr.close()

                if self.cfg.tracking:
                    self.log.track(_lev + 1, "Reset Connection Closed.", True)
            except Exception as e:
                rtrn = False
                enum = "DB198"
                emsg = "close(): [" + str(e) + "]"
                if self.cfg.tracking:
                    self.log.track(_lev + 1, str(enum) + ": " + str(emsg), True)

        if self.dbc:
            try:
                self.dbc.close()
                self.dbc = None

                if self.cfg.tracking:
                    self.log.track(_lev + 1, "Main Connection Closed.", True)
            except Exception as e:
                rtrn = False
                enum = "DB199"
                emsg = "close(): [" + str(e) + "]"
                if self.cfg.tracking:
                    self.log.track(_lev + 1, str(enum) + ": " + str(emsg), True)

        return [rtrn, enum, emsg]
