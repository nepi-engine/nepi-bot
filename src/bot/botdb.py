########################################################################
##
##  Module: botdb.py
##  ----------------
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
##  Revision:   1.13 2019/04/15  11:00:00
##  Comment:    Upgrade to new Node Type'code' Converison Table.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
##  Revision:   1.12 2019/04/12  11:20:00
##  Comment:    Changed Reporting levels to 12/24.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
##  Revision:   1.11 2019/04/10  08:00:00
##  Comment:    Add 'eligible' Table to manage Cloud tracking.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
##  Revision:   1.10 2019/04/04  08:00:00
##  Comment:    General updates/fixes to support 5/1 Data Products.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
##  Revision:   1.9 2019/04/03  08:15:00
##  Comment:    Remove 'admin' Table Mgmt; Fix Err Mgmt.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
##  Revision:   1.8 2019/04/02  13:30:00
##  Comment:    Fix getconn() success return codes.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
##  Revision:   1.7 2019/03/22  14:30:00
##  Comment:    Fix getconn() success return codes.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
##  Revision:   1.6 2019/03/14  10:20:00
##  Comment:    Added S/W Rev increment management.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
##  Revision:   1.5 2019/02/21  12:20:00
##  Comment:    Added 'admin' Table management.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
##  Revision:   1.4 2019/02/19  09:30:00
##  Comment:    Added SELECT and UPDATE method functionality.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
##  Revision:   1.3 2019/02/13  11:30:00
##  Comment:    Upgrade to new debugging/logging model; plus test.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
##  Revision:   1.2 2019/02/04  08:06:00
##  Comment:    Constructor; database creation.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
##  Revision:   1.1 2019/01/13  12:00:00
##  Comment:    Module Instantiation.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
########################################################################

import os
import sqlite3

#-------------------------------------------------------------------
def dict_factory(cursor, row):
#-------------------------------------------------------------------
    # This code will help extract a group of dictionaries based on
    # the SELECT statement from the SQLite database and dump it into
    # JSON for use.
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

########################################################################
##  The Float Database Class
########################################################################

class BotDB(object):

    def __init__(self, _cfg, _log, _lev):
        self.dbc = None
        self.dbr = None
        self.cfg = _cfg
        self.log = _log
        if self.cfg.tracking:
            self.log.track(_lev, 'Created DB Class Object: ', True)
            self.log.track(_lev+13, '^dbc: ' + str(self.dbc), True)
            self.log.track(_lev+13, '^dbr: ' + str(self.dbr), True)
            self.log.track(_lev+13, '^log: ' + str(self.log), True)
            self.log.track(_lev+13, '^cfg: ' + str(self.cfg), True)
            self.log.track(_lev+13, '_lev: ' + str(_lev), True)

    def getconn(self, _lev):
        # Establish a connection. SQLite either creates or opens a file
        # (in this case, the 'dbfile' passed to the SQLite class method
        # above).  If the file is there, we assume that it is a legit
        # Float Database.  If not there, the DB requires instantiation.
        if self.cfg.tracking:
            self.log.track(_lev, "Entering DB 'getconn()' Method.", True)
            self.log.track(_lev+1, "Establish DB Connection.", True)

        if os.path.isfile(self.cfg.db_file):
            if self.cfg.tracking:
                self.log.track(_lev+2, "DB File Found: " + str(self.cfg.db_file), True)
        else:
            if self.cfg.tracking:
                self.log.track(_lev+2, "DB File NOT Found: " + str(self.cfg.db_file), True)
            success = self.reset(_lev+2)
            if not success[0]:
                return success, None

        try:
            self.dbc = sqlite3.connect(self.cfg.db_file)
            if self.cfg.tracking:
                self.log.track(_lev+1, "DB Connection Established.", True)
            return [ True, None, None ], self.dbc
        except Exception as e:
            enum = "DB001"
            emsg = "getconn(): [" + str(e) + "]"
            if self.cfg.tracking:
                self.log.errtrack(str(enum), str(emsg))
            return [ False, str(enum), str(emsg) ], None

    #-------------------------------------------------------------------
    def pushstat(self, _lev, _statjson):
        # INSERT a 'status' record into the Float DB.  This Module is
        # designed to insert the exact number of columns by picking
        # apart the 'statjson' JSON-formatted 'Status' record as
        # retrieved from the Data Folder and subjected to a json.load.
        if self.cfg.tracking:
            self.log.track(_lev, "Entering DB 'pushstat()' Module.", True)
            self.log.track(_lev+1, "_lev: " + str(_lev), True)
            self.log.track(_lev+13, "_JSON: " + str(_statjson), True)
            self.log.track(_lev+1, "Format SQL INSERT Statement.", True)

        #---------------------------------------------------------------
        # Create the SQL statement for inserting the Status Record into
        # the 'float.db' database.  This includes a combination of JSON
        # values from the SDK's Status File in the Data Folder PLUS a
        # handfull of seeded control values for columns (fields) that
        # will be used later.
        try:
            sql = "INSERT INTO status VALUES (0,0,0,NULL,NULL,%.15f,'%s','%s',0,%.15f,%.15f,%15f,%.15f,%.15f,%.15f,%.15f,%i,%i,%i,%i,%i,%i,%i,%i,%i,%i,NULL,NULL)" % (
                float(_statjson['timestamp']),
                str(_statjson['serial_num']),
                str(_statjson["sw_rev"]),
                float(_statjson["navsat_fix_time"]),
                float(_statjson["latitude"]),
                float(_statjson["longitude"]),
                float(_statjson["heading"]),
                float(_statjson["batt_charge"]),
                float(_statjson["bus_voltage"]),
                float(_statjson["temperature"]),
                int(_statjson["trig_wake_count"]),
                int(_statjson["wake_event_type"]),
                int(_statjson["wake_event_id"]),
                int(_statjson["task_index"]),
                int(_statjson["trig_cfg_index"]),
                int(_statjson["rule_cfg_index"]),
                int(_statjson["sensor_cfg_index"]),
                int(_statjson["node_cfg_index"]),
                int(_statjson["geofence_cfg_index"]),
                int(_statjson["state_flags"])
            )
        except Exception as e:
            enum = "DB002"
            emsg = "pushstat(): [" + str(e) + "]"
            if self.cfg.tracking:
                self.log.errtrack(str(enum), str(emsg))
            return [ False, str(enum), str(emsg) ], None

        #---------------------------------------------------------------
        # Perform the actual INSERT and 'commit()' the new record to the
        # 'float.db' database.
        if self.cfg.tracking:
            self.log.track(_lev+1, "INSERT Status Record into DB.", True)
            self.log.track(_lev+14, "SQL: " + str(sql), True)
        
        try:
            cursor = self.dbc.cursor()
            cursor.execute(str(sql))
            lastrowid = cursor.lastrowid
            if self.cfg.tracking:
                self.log.track(_lev+14, "Successful.", True)
                self.log.track(_lev+14, "Row ID: " + str(lastrowid), True)
            self.dbc.commit()
        except Exception as e:
            enum = "DB003"
            emsg = "pushstat(): [" + str(e) + "]"
            if self.cfg.tracking:
                self.log.errtrack(str(enum), str(emsg))
            return [ False, str(enum), str(emsg) ], None

        return [ True, None, None ], lastrowid

    #-------------------------------------------------------------------
    # INSERT a 'Data' record into the Float DB. This Module is designed
    # to insert the exact number of columns by picking apart the
    # 'datajson' JSON-formatted 'Meta' record as retrieved from the
    # Data Folder and subjected to a json.load.  In addition, the
    # mandatory "standard" and optional "change" files are included.
    #def pushmeta(self, _lev, _datajson, _state, _status, _trigger, _numerator, _stdsize, _chgsize, _norm, _pipo, _metafile, _stdfile, _chgfile):
    def pushmeta(self, _lev, _datajson, _info, _status, _trigger, _metafile):
        if self.cfg.logging:
            self.log.track(_lev, "Entering DB 'popdata()' Module.", True)
            self.log.track(_lev+1, "Log Level:    " + str(_lev), True)
            self.log.track(_lev+13, "Data JSON:    " + str(_datajson), True)
            self.log.track(_lev+1, "State:        " + str(_info[3]), True)
            self.log.track(_lev+1, "Status ID:    " + str(_status), True)
            self.log.track(_lev+1, "Trigger:      " + str(_trigger), True)
            self.log.track(_lev+1, "Numerator:    " + str(_info[0]), True)
            self.log.track(_lev+1, "Std Size:     " + str(_info[1]), True)
            self.log.track(_lev+1, "Chg Size:     " + str(_info[2]), True)
            self.log.track(_lev+1, "Normalize:    " + str(_info[4]), True)
            self.log.track(_lev+1, "PIPO Rating:  " + str(_info[5]), True)
            self.log.track(_lev+1, "MetaFile:     " + str(_metafile), True)
            self.log.track(_lev+1, "StdFile:      " + str(_info[6]), True)
            self.log.track(_lev+1, "ChgFile:      " + str(_info[7]), True)
            self.log.track(_lev+1, "Chg Eligible: " + str(_info[3]), True)
            self.log.track(_lev+13, "StdJSON:      " + str(_info[8]), True)
            self.log.track(_lev+13, "ChgJSON:      " + str(_info[9]), True)
            self.log.track(_lev+1, "Format SQL INSERT Statement.", True)

        chg_channels = 0
        chg_dtype = ""
        chg_deltas = ""

        try:
            if _info[9]:
                chg_channels = int(_info[9]["channel_count"])
                chg_dtype    = str(_info[9]["dt"])
                chg_deltas   = str(_info[9]["deltas"])
        except Exception as e:
            enum = "DB104"
            emsg = "pushmeta(): [" + str(e) + "]"
            if self.cfg.tracking:
                self.log.errtrack(str(enum), str(emsg))
            return [ False, str(enum), str(emsg) ], None

        #---------------------------------------------------------------
        # Create the SQL statement for inserting the Data Record into
        # the 'float.db' database.  This includes a combination of JSON
        # values from the SDK's Meta File in the Data Folder PLUS a
        # handfull of seeded control values for columns (fields) that
        # will be used later and specific values passed to this function
        # as arguments.
        try:
            sql = "INSERT INTO meta VALUES (%i,0,%i,%.15f,%.15f,%.15f,'%s',%i,%.15f,%.15f,%.15f,%.15f,'%s','%s','%s',%i,%i,%i,'%s','%s',%i,%i,%i,%i,%i,%i,%i,'%s','%s',%i,'%s','%s',%i)" % (
                _info[3],
                _status,
                _info[0],
                _trigger,
                _info[5],
                str(_datajson['node_type']),
                int(_datajson['instance']),
                float(_datajson['timestamp']),
                float(_datajson["heading"]),
                float(_datajson["quality"]),
                float(_datajson["node_id_score"]),
                _metafile,
                str(_datajson["data_file"]),
                str(_datajson["change_file"]),
                _info[1],
                _info[2],
                _info[4],
                _info[6],
                _info[7],
                _info[3],
                int(_datajson["pitch"]),
                int(_datajson["roll"]),
                int(_info[8]["channels"]),
                int(_info[8]["rows"]),
                int(_info[8]["cols"]),
                int(_info[8]["samples"]),
                str(_info[8]["dtype"]),
                str(_info[8]["data"]),
                int(chg_channels),
                str(chg_dtype),
                str(chg_deltas),
                int(_info[10])
            )
        except Exception as e:
            enum = "DB004"
            emsg = "pushmeta(): [" + str(e) + "]"
            if self.cfg.tracking:
                self.log.errtrack(str(enum), str(emsg))
            return [ False, str(enum), str(emsg) ], None

        #---------------------------------------------------------------
        # Perform the actual INSERT and 'commit()' the new 'meta' record
        # to the 'float.db' database.
        if self.cfg.tracking:
            self.log.track(_lev+13, "SQL: " + str(sql), True)
            self.log.track(_lev, "Perform the 'Meta' Record INSERT.", True)

        try:
            cursor = self.dbc.cursor()
            cursor.execute(str(sql))
            lastrowid = cursor.lastrowid
            if self.cfg.tracking:
                self.log.track(_lev+1, "Data Row ID: " + str(lastrowid), True)
            self.dbc.commit()
        except Exception as e:
            enum = "DB005"
            emsg = "pushmeta(): [" + str(e) + "]"
            if self.cfg.tracking:
                self.log.errtrack(str(enum), str(emsg))
            return [ False, str(enum), str(emsg) ], None

        return [ True, None, None ], lastrowid


    #-------------------------------------------------------------------
    def getResults(self, _lev, _sql, _jflag):
    #-------------------------------------------------------------------
        if self.cfg.tracking:
            self.log.track(_lev, "Entering DB getResults() Module.", True)
            self.log.track(_lev+1, "_lev:   " + str(_lev), True)
            self.log.track(_lev+13, "_sql:   " + str(_sql), True)
            self.log.track(_lev+1, "_jflag: " + str(_jflag), True)

        try:
            if _jflag:
                rowfac = self.dbc.row_factory
                self.dbc.row_factory = dict_factory
                if self.cfg.tracking:
                    self.log.track(_lev+1, "Changed Row Factory.", True)
            
            cursor = self.dbc.cursor()
            if self.cfg.tracking:
                self.log.track(_lev+1, "Got Cursor.", True)
            cursor.execute(str(_sql))
            if self.cfg.tracking:
                self.log.track(_lev+1, "SQL Executed.", True)
            results = cursor.fetchall()
            if self.cfg.tracking:
                self.log.track(_lev+1, "Fetched Rows.", True)

            if _jflag:
                self.dbc.row_factory = rowfac
                if self.cfg.tracking:
                    self.log.track(_lev+1, "Changed Row Factory Back.", True)
        except Exception as e:
            enum = "DB006"
            emsg = "getResults(): [" + str(e) + "]"
            if self.cfg.tracking:
                self.log.errtrack(str(enum), str(emsg))
            return [ False, str(enum), str(emsg) ], None

        if self.cfg.tracking:
            self.log.track(_lev+1, "Returning Result Set.", True)
            self.log.track(_lev+13, "Results: " + str(results), True)

        return [ True, None, None ], results

    #-------------------------------------------------------------------
    def update(self, _lev, _sql):
    #-------------------------------------------------------------------
        if self.cfg.tracking:
            self.log.track(_lev, "Entering DB update() Module.", True)
            self.log.track(_lev+1, "_lev: " + str(_lev), True)
            self.log.track(_lev+13, "_sql: " + str(_sql), True)

        try:
            cursor = self.dbc.cursor()
            if self.cfg.tracking:
                self.log.track(_lev+1, "Got Cursor.", True)
            cursor.execute(str(_sql))
            if self.cfg.tracking:
                self.log.track(_lev+1, "SQL Executed.", True)
            self.dbc.commit()
            if self.cfg.tracking:
                self.log.track(_lev+1, "Update Committed.", True)
        except Exception as e:
            enum = "DB007"
            emsg = "update(): [" + str(e) + "]"
            if self.cfg.tracking:
                self.log.errtrack(str(enum), str(emsg))
            return [ False, str(enum), str(emsg) ]

        return [ True, None, None ]

    #-------------------------------------------------------------------
    def reset(self, _lev):
    #-------------------------------------------------------------------
    # WARNING!  A call to this method WILL completely destroy and, then,
    # re-instantiate the ENTIRE Float DB.  This is used primarily during
    # development and testing, but can also be activated if a 'command
    # and control' message is sent from "the Cloud" during deployment
    # (e.g., when a complete Float reset is deemed necessary).
        if self.cfg.tracking:
            self.log.track(_lev, "Entering DB reset() Module.", True)
            self.log.track(_lev+1, "_lev: " + str(_lev), True)

        try:
            # If the Float Database file exists, delete it.  If we can't
            # get rid of it, this App is dead in the water, at least for
            # now. In later revisions, we can create an memory-resident
            # DB so we can limp along sans Data Product retention (after
            # notifying "the Cloud" of the condition).
            if os.path.isfile(self.cfg.db_file):
                if self.cfg.tracking:
                    self.log.track(_lev+1, "DB File Exists; Remove it.", True)
                try:
                    os.remove(self.cfg.db_file)
                    self.log.track(_lev+2, "Removed DB File.", True)
                except Exception as e:
                    enum = "DB101"
                    emsg = "reset(): [" + str(e)  + "]"
                    if self.cfg.tracking:
                        self.log.errtrack(str(enum), str(emsg))
                    return [ False, str(enum), str(emsg) ]
            else:
                if self.cfg.tracking:
                    self.log.track(_lev+1, "DB File does not exist; Continue.", True)

            # Create the 'Database Directory' in case there wasn't one
            # to begin with.  This should never happen and requires
            # that the 'NEPI Home' directory is writable; if not, this
            # attempt will fail, rendering the entire DB functionality
            # moot.  The test for existence eliminates the possibilty
            # of os.makeidrs() failure due to the directory already
            # existing - a condition which is benign.
            dir = os.path.dirname(self.cfg.db_file)
            if self.cfg.tracking:
                self.log.track(_lev+1, "Check DB Directory.", True)
                self.log.track(_lev+2, "Dir: " + str(dir), True)

            if not os.path.isdir(str(dir)):
                if self.cfg.tracking:
                    self.log.track(_lev+2, "DB Directory doesn't exist.", True)
                try:
                    os.makedirs(str(dir))
                    if self.cfg.tracking:
                        self.log.track(_lev+2, "Created Dir: " + str(dir), True)
                except Exception as e:
                    enum = "DB102"
                    emsg = "reset(): [" + str(e)  + "]"
                    if self.cfg.tracking:
                        self.log.errtrack(str(enum), str(emsg))
                    return [ False, str(enum), str(emsg) ]
            else:
                if self.cfg.tracking:
                    self.log.track(_lev+2, "DB Directory Exists.", True)

            #-----------------------------------------------------------
            # Open a first connection with the DB (creates the DB file).
            if self.cfg.tracking:
                self.log.track(_lev+1, "Connect with New Database.", True)

            try:        
                self.dbr = sqlite3.connect(self.cfg.db_file)
            except Exception as e:
                enum = "DB103"
                emsg = "reset(): [" + str(e)  + "]"
                if self.cfg.tracking:
                    self.log.errtrack(str(enum), str(emsg))
                return [ False, str(enum), str(emsg) ]

            #-----------------------------------------------------------
            # Get a cursor object for upcoming Table and Index creation
            # activity.
            if self.cfg.tracking:
                self.log.track(_lev+1, "Get the DB Cursor.", True)

            try:
                cursor = self.dbr.cursor()
            except:
                enum = "DB104"
                emsg = "reset(): [" + str(e)  + "]"
                if self.cfg.tracking:
                    self.log.errtrack(str(enum), str(emsg))
                return [ False, str(enum), str(emsg) ]

            #-----------------------------------------------------------
            # Create the 'status' Table.
            if self.cfg.tracking:
                self.log.track(_lev+1, "Create the 'status' Table.", True)

            try:
                cursor.execute("""CREATE TABLE status
                    (state TINYINT,
                    base BOOLEAN,
                    meta_state INTEGER,
                    reserved1 BLOB,
                    reserved2 BLOB,
                    timestamp DOUBLE PRECISION,
                    serial_num TEXT,
                    sw_rev TEXT,
                    sw_rev_increment TINYINT, 
                    navsat_fix_time DOUBLE PRECISION,
                    latitude DOUBLE PRECISION,
                    longitude DOUBLE PRECISION,
                    heading DOUBLE PRECISION,
                    batt_charge DOUBLE PRECISION,
                    bus_voltage DOUBLE PRECISION,
                    temperature DOUBLE PRECISION,
                    trig_wake_count SMALLINT,
                    wake_event_type TINYINT,
                    wake_event_id SMALLINT,
                    task_index SMALLINT,
                    trig_cfg_index SMALLINT,
                    rule_cfg_index SMALLINT,
                    sensor_cfg_index SMALLINT,
                    node_cfg_index SMALLINT,
                    geofence_cfg_index SMALLINT,
                    state_flags SMALLINT,
                    reserved3 BLOB,
                    reserved4 BLOB) 
                    """)
            except:
                enum = "DB105"
                emsg = "reset(): [" + str(e)  + "]"
                if self.cfg.tracking:
                    self.log.errtrack(str(enum), str(emsg))
                return [ False, str(enum), str(emsg) ]

            #-----------------------------------------------------------
            # Create the 'meta' Table.
            if self.cfg.tracking:
                self.log.track(_lev+1, "Create the 'meta' Table.", True)

            try:
                cursor.execute("""CREATE TABLE meta
                    (state TINYINT,
                    base BOOLEAN,
                    status BIGINT,
                    numerator DOUBLE PRECISION,
                    trigger DOUBLE PRECISION,
                    pipo DOUBLE PRECISION,
                    type TEXT,
                    instance SMALLINT,
                    timestamp DOUBLE PRECISION,
                    heading DOUBLE PRECISION,
                    quality DOUBLE PRECISION,
                    score DOUBLE PRECISION,
                    metafile STRING,
                    stdfile STRING,
                    chgfile STRING,
                    stdsize INTEGER,
                    chgsize INTEGER,
                    norm DOUBLE PRECISION,
                    std TEXT,
                    chg TEXT,
                    chg_eligible BOOLEAN,
                    pitch INTEGER,
                    roll INTEGER,
                    channels INTEGER,
                    rows INTEGER,
                    columns INTEGER,
                    samples INTEGER,
                    dtype TEXT,
                    data TEXT,
                    chg_channels INTEGER,
                    chg_dtype TEXT,
                    chg_deltas TEXT,
                    node_code INTEGER)
                    """)
            except  Exception as e:
                enum = "DB106"
                emsg = "reset(): [" + str(e)  + "]"
                if self.cfg.tracking:
                    self.log.errtrack(str(enum), str(emsg))
                return [ False, str(enum), str(emsg) ]

            #-----------------------------------------------------------
            # Create the 'node' Table.
            #-----------------------------------------------------------
            if self.cfg.tracking:
                self.log.track(_lev+1, "Create the 'node' Table.", True)

            try:
                cursor.execute("""CREATE TABLE node
                    (node_type TEXT,
                    node_code TINYINT,
                    node_instance TINYINT,
                    node_index TINYINT,
                    node_stage TINYINT)
                    """)
            except  Exception as e:
                enum = "DB107"
                emsg = "reset(): [" + str(e)  + "]"
                if self.cfg.tracking:
                    self.log.errtrack(str(enum), str(emsg))
                return [ False, str(enum), str(emsg) ]

            #-----------------------------------------------------------
            # Instantiate the 'node' Table.
            #-----------------------------------------------------------
            cmds = [ "INSERT INTO node VALUES ('BAT', 1, 0, 0, 0);",
                    "INSERT INTO node VALUES ('THR', 2, 0, 0, 0);",
                    "INSERT INTO node VALUES ('IMU', 3, 0, 0, 0);",
                    "INSERT INTO node VALUES ('GPS', 5, 0, 0, 0);",
                    "INSERT INTO node VALUES ('CAM', 9, 0, 0, 0);",
                    "INSERT INTO node VALUES ('ESR', 10, 0, 0, 0);",
                    "INSERT INTO node VALUES ('ESP', 11, 0, 0, 0);",
                    "INSERT INTO node VALUES ('EBI', 12, 0, 0, 0);",
                    "INSERT INTO node VALUES ('ECH', 13, 0, 0, 0);",
                    "INSERT INTO node VALUES ('ERD', 14, 0, 0, 0);",
                    "INSERT INTO node VALUES ('EPR', 15, 0, 0, 0);",
                    "INSERT INTO node VALUES ('EPC', 16, 0, 0, 0);",
                    "INSERT INTO node VALUES ('CTP', 17, 0, 0, 0);",
                    "INSERT INTO node VALUES ('CRS', 18, 0, 0, 0);",
                    "INSERT INTO node VALUES ('CBD', 19, 0, 0, 0);",
                    "INSERT INTO node VALUES ('CAV', 20, 0, 0, 0);",
                    "INSERT INTO node VALUES ('CCA', 21, 0, 0, 0);",
                    "INSERT INTO node VALUES ('CRB', 22, 0, 0, 0);",
                    "INSERT INTO node VALUES ('CGS', 23, 0, 0, 0);",
                    "INSERT INTO node VALUES ('CGP', 24, 0, 0, 0);",
                    "INSERT INTO node VALUES ('UGP', 25, 0, 0, 0);",
                    "INSERT INTO node VALUES ('NBD', 26, 0, 0, 0);",
                    "INSERT INTO node VALUES ('NLG', 27, 0, 0, 0);",
                    "INSERT INTO node VALUES ('FTH', 28, 0, 0, 0);",
                    "INSERT INTO node VALUES ('FCF', 29, 0, 0, 0);",
                    "INSERT INTO node VALUES ('FTM', 30, 0, 0, 0);",
                    "INSERT INTO node VALUES ('FWN', 31, 0, 0, 0);",
                    "INSERT INTO node VALUES ('FRS', 32, 0, 0, 0);",
                    "INSERT INTO node VALUES ('FBB', 33, 0, 0, 0);",
                    "INSERT INTO node VALUES ('FSG', 34, 0, 0, 0);",
                    "INSERT INTO node VALUES ('FPK', 35, 0, 0, 0);",
                    "INSERT INTO node VALUES ('FOF', 36, 0, 0, 0);",
                    "INSERT INTO node VALUES ('FDN', 37, 0, 0, 0);",
                    "INSERT INTO node VALUES ('RCH', 38, 0, 0, 0);",
                    "INSERT INTO node VALUES ('CLA', 39, 0, 0, 0);",
                    "INSERT INTO node VALUES ('SIN', 40, 0, 0, 0);"
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
                    emsg = "reset(): [" + str(e)  + "]"
                    if self.cfg.tracking:
                        self.log.errtrack(str(enum), str(emsg))
                    return [ False, str(enum), str(emsg) ], None

            #-----------------------------------------------------------
            # Close the Database.
            #-----------------------------------------------------------
            if self.cfg.tracking:
                self.log.track(_lev+1, "Close the Database.", True)

            try:
                self.dbr.close()
                self.dbr = None
                if self.cfg.tracking:
                    self.log.track(_lev+2, "Closed.", True)
            except Exception as e:
                if self.cfg.tracking:
                   self.log.track(_lev+2, "WARNING: DB Failed to Close Properly.", True)
                   self.log.track(_lev+2, "ERR MSG: [" + str(e) + "]", True)
            
            return [ True, None, None ]

        except Exception as e:
            if self.cfg.tracking:
                enum = "DB108"
                emsg = "reset(): [" + str(e)  + "]"
                if self.cfg.tracking:
                    self.log.errtrack(str(enum), str(emsg))
                return [ False, str(enum), str(emsg) ]

            