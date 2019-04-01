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
##  Revision:   1.7 2019/03/22  13:30:00
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

    def __init__(self, cfg, log):
        self.dbc = None
        self.dbr = None
        self.cfg = cfg
        self.log = log
        if self.cfg.tracking:
            log.track(0, 'Created DB Class Object: ', True)
            log.track(1, 'dbc: ' + str(self.dbc), True)
            log.track(1, 'dbr: ' + str(self.dbr), True)
            log.track(1, 'log: ' + str(self.log), True)
            log.track(1, 'cfg: ' + str(self.cfg), True)

    def getconn(self):
        # Establish a connection. SQLite either creates or opens a file
        # (in this case, the 'dbfile' passed to the SQLite class method
        # above).  If the file is there, we assume that it is a legit
        # Float Database.  If not there, the DB requires instantiation.
        if self.cfg.tracking:
            self.log.track(1, "Establish Connection.", True)

        if os.path.isfile(self.cfg.db_file):
            if self.cfg.tracking:
                self.log.track(2, "DB File Found: " + str(self.cfg.db_file), True)
        else:
            if self.cfg.tracking:
                self.log.track(2, "DB File NOT Found: " + str(self.cfg.db_file), True)
            success = self.reset()
            if not success:
                return [ False, None, None ], None

        try:
            self.dbc = sqlite3.connect(self.cfg.db_file)
            if self.cfg.tracking:
                self.log.track(2, "DB Connection Established: " + str(self.cfg.db_file), True)
            return [ True, None, None ], self.dbc
        except Exception as e:
            if self.cfg.tracking:
                self.log.errtrack("DB001", "Connection FAILED: " + str(e) + "]" )
            return [ False, "DB001", "Connection FAILED: " + str(e) + "]" ], None

    #-------------------------------------------------------------------
    def pushstat(self, lev, statjson):
        # INSERT a 'status' record into the Float DB.  This Module is
        # designed to insert the exact number of columns by picking
        # apart the 'statjson' JSON-formatted 'Status' record as
        # retrieved from the Data Folder and subjected to a json.load.
        if self.cfg.tracking:
            self.log.track(lev, "Entering 'popstat()' Module.", True)
            self.log.track(lev+1, "Log Level:   " + str(lev), True)
            self.log.track(lev+6, "Status JSON: " + str(statjson), True)

        #---------------------------------------------------------------
        # Create the SQL statement for inserting the Status Record into
        # the 'float.db' database.  This includes a combination of JSON
        # values from the SDK's Status File in the Data Folder PLUS a
        # handfull of seeded control values for columns (fields) that
        # will be used later.
        sql = "INSERT INTO status VALUES (0,0,0,NULL,NULL,%.15f,'%s','%s',0,%.15f,%.15f,%15f,%.15f,%.15f,%.15f,%.15f,%i,%i,%i,%i,%i,%i,%i,%i,%i,%i,NULL,NULL)" % (
            float(statjson['timestamp']),
            str(statjson['serial_num']),
            str(statjson["sw_rev"]),
            float(statjson["navsat_fix_time"]),
            float(statjson["latitude"]),
            float(statjson["longitude"]),
            float(statjson["heading"]),
            float(statjson["batt_charge"]),
            float(statjson["bus_voltage"]),
            float(statjson["temperature"]),
            int(statjson["trig_wake_count"]),
            int(statjson["wake_event_type"]),
            int(statjson["wake_event_id"]),
            int(statjson["task_index"]),
            int(statjson["trig_cfg_index"]),
            int(statjson["rule_cfg_index"]),
            int(statjson["sensor_cfg_index"]),
            int(statjson["node_cfg_index"]),
            int(statjson["geofence_cfg_index"]),
            int(statjson["state_flags"])
        )

        #---------------------------------------------------------------
        # Perform the actual INSERT and 'commit()' the new record to the
        # 'float.db' database.
        if self.cfg.tracking:
            self.log.track(lev, "Perform the 'Status' Record INSERT.", True)
            self.log.track(lev+6, "SQL: " + str(sql), True)
        
        try:
            cursor = self.dbc.cursor()
            cursor.execute(str(sql))
            lastrowid = cursor.lastrowid
            if self.cfg.tracking:
                self.log.track(lev+1, "Row ID" + str(lastrowid), True)
            self.dbc.commit()
        except Exception as e:
            if self.cfg.tracking:
                self.log.errtrack("DB002", "DB INSERT FAILED.")
                self.log.errtrack("DB001", "EXCEPTION: [" + str(e) + "]")
            return [ False, "DB002", "EXCEPTION: [" + str(e) + "]" ]

        return [ True, None, None ], lastrowid

    #-------------------------------------------------------------------
    # INSERT a 'Data' record into the Float DB. This Module is designed
    # to insert the exact number of columns by picking apart the
    # 'datajson' JSON-formatted 'Meta' record as retrieved from the
    # Data Folder and subjected to a json.load.  In addition, the
    # mandatory "standard" and optional "change" files are included.
    def pushmeta(self, lev, datajson, state, status, trigger, numerator, stdsize, chgsize, norm, pipo, metafile, stdfile, chgfile):
        if self.cfg.logging:
            self.log.track(lev, "Entering 'popdata()' Module.", True)
            self.log.track(lev+1, "Log Level:   " + str(lev), True)
            self.log.track(lev+6, "Data JSON:   " + str(datajson), True)
            self.log.track(lev+1, "State:       " + str(state), True)
            self.log.track(lev+1, "Status ID:   " + str(status), True)
            self.log.track(lev+1, "Trigger:     " + str(trigger), True)
            self.log.track(lev+1, "Numerator:   " + str(numerator), True)
            self.log.track(lev+1, "Std Size:    " + str(stdsize), True)
            self.log.track(lev+1, "Chg Size:    " + str(chgsize), True)
            self.log.track(lev+1, "Normalize:   " + str(norm), True)
            self.log.track(lev+1, "PIPO Rating: " + str(pipo), True)
            self.log.track(lev+1, "MetaFile:    " + str(metafile), True)
            self.log.track(lev+1, "StdFile:     " + str(stdfile), True)
            self.log.track(lev+1, "ChgFile:     " + str(chgfile), True)

        #---------------------------------------------------------------
        # Create the SQL statement for inserting the Data Record into
        # the 'float.db' database.  This includes a combination of JSON
        # values from the SDK's Meta File in the Data Folder PLUS a
        # handfull of seeded control values for columns (fields) that
        # will be used later and specific values passed to this function
        # as arguments.
        sql = "INSERT INTO meta VALUES (%i,0,%i,%.15f,%.15f,%.15f,'%s',%i,%.15f,%.15f,%.15f,%.15f,'%s','%s','%s',%i,%i,%i,'%s','%s')" % (
            state,
            status,
            numerator,
            trigger,
            pipo,
            str(datajson['node_type']),
            int(datajson['instance']),
            float(datajson['timestamp']),
            float(datajson["heading"]),
            float(datajson["quality"]),
            float(datajson["node_id_score"]),
            metafile,
            str(datajson["data_file"]),
            str(datajson["change_file"]),
            stdsize,
            chgsize,
            norm,
            stdfile,
            chgfile
        )

        #---------------------------------------------------------------
        # Perform the actual INSERT and 'commit()' the new record to the
        # 'float.db' database.
        if self.cfg.tracking:
            self.log.track(lev, "Perform the Data Record INSERT.", True)
            self.log.track(lev+6, "SQL: " + str(sql), True)

        try:
            cursor = self.dbc.cursor()
            lastrowid = cursor.execute(str(sql))
            if self.cfg.tracking:
                self.log.track(lev+1, "Data Row ID" + str(lastrowid), True)
            self.dbc.commit()
        except Exception as e:
            if self.cfg.tracking:
                self.log.errtrack("DB003", "DB DATA INSERT FAILED.")
                self.log.errtrack("DB003", "EXCEPTION: [" + str(e) + "]")
            return [ False, "DB003", "EXCEPTION: [" + str(e) + "]" ]

        return [ True, None, None ], lastrowid


    #-------------------------------------------------------------------
    def getResults(self, lev, sql, jflag):
    #-------------------------------------------------------------------
        if self.cfg.tracking:
            self.log.track(lev, "Entering DB getResults() Module.", True)
            self.log.track(lev+1, "lev:   " + str(lev), True)
            self.log.track(lev+6, "sql:   " + str(sql), True)
            self.log.track(lev+6, "jflag: " + str(jflag), True)

        try:
            if jflag:
                rowfac = self.dbc.row_factory
                self.dbc.row_factory = dict_factory
                if self.cfg.tracking:
                    self.log.track(lev+1, "Changed Row Factory.", True)
            
            cursor = self.dbc.cursor()
            if self.cfg.tracking:
                self.log.track(lev+1, "Got Cursor.", True)
            cursor.execute(str(sql))
            if self.cfg.tracking:
                self.log.track(lev+1, "SQL Executed.", True)
            results = cursor.fetchall()
            if self.cfg.tracking:
                self.log.track(lev+1, "Fetched Rows.", True)

            if jflag:
                self.dbc.row_factory = rowfac
                if self.cfg.tracking:
                    self.log.track(lev+1, "Changed Row Factory Back.", True)
        except Exception as e:
            if self.cfg.tracking:
                self.log.errtrack("DB109", "Results failure: " + str(e))
            return [ False, "DB109", "Results failure: " + str(e)], None

        if self.cfg.tracking:
            self.log.track(lev+1, "Returning Result Set.", True)
            self.log.track(lev+7, "Results: " + str(results), True)

        return [ True, None, None ], results

    #-------------------------------------------------------------------
    def update(self, lev, sql):
    #-------------------------------------------------------------------
        if self.cfg.tracking:
            self.log.track(lev, "Entering DB update() Module.", True)
            self.log.track(lev+1, "lev: " + str(lev), True)
            self.log.track(lev+6, "sql: " + str(sql), True)

        try:
            cursor = self.dbc.cursor()
            if self.cfg.tracking:
                self.log.track(lev+1, "Got Cursor.", True)
            cursor.execute(str(sql))
            if self.cfg.tracking:
                self.log.track(lev+1, "SQL Executed.", True)
            self.dbc.commit()
            if self.cfg.tracking:
                self.log.track(lev+1, "Update Committed.", True)
        except Exception as e:
            if self.cfg.tracking:
                self.log.errtrack("DB110", "Update failure: " + str(e))
            return [ False, "DB110", "Update failure: " + str(e) ]

        return [ True, None, None ]

    #-------------------------------------------------------------------
    def reset(self):
    #-------------------------------------------------------------------
    # WARNING!  A call to this method WILL completely destroy and, then,
    # re-instantiate the ENTIRE Float DB.  This is used primarily during
    # development and testing, but can also be activated if a 'command
    # and control' message is sent from "the Cloud" during deployment
    # (e.g., when a complete Float reset is deemed necessary).
        if self.cfg.tracking:
            self.log.track(3, "Entering DB reset() Module.", True)

        try:
            # If the Float Database file exists, delete it.  If we can't
            # get rid of it, this App is dead in the water, at least for
            # now. In later revisions, we can create an memory-resident
            # DB so we can limp along sans Data Product retention (after
            # notifying "the Cloud" of the condition).
            if os.path.isfile(self.cfg.db_file):
                if self.cfg.tracking:
                    self.log.track(3, "DB File Exists; Remove it.", True)
                try:
                    os.remove(self.cfg.db_file)
                    self.log.track(4, "Removed DB File.", True)
                except:
                    if self.cfg.tracking:
                        self.log.errtrack("DB101", "Can't remove existing DB File: " + str(self.cfg.db_file))
                    return [ False, "DB101", "Can't remove existing DB File: " + str(self.cfg.db_file) ], None
            else:
                if self.cfg.tracking:
                    self.log.track(3, "DB File does not exist.", True)

            # Create the 'Database Directory' in case there wasn't one
            # to begin with.  This should never happen and requires
            # that the 'NEPI Home' directory is writable; if not, this
            # attempt will fail, rendering the entire DB functionality
            # moot.  The test for existence eliminates the possibilty
            # of os.makeidrs() failure due to the directory already
            # existing - a condition which is benign.
            if not os.path.isdir(os.path.dirname(self.cfg.db_file)):
                try:
                    os.makedirs(os.path.dirname(self.cfg.db_file))
                    if self.cfg.tracking:
                        self.log.track(3, "DB Dir didn't exist; Made: " + str(os.path.dirname(self.cfg.db_file)), True)
                except:
                    if self.cfg.tracking:
                        self.log.errtrack("DB102", "Can't make DB Dir: " + str(os.path.dirname(self.cfg.db_file)))
                    return [ False, "Can't make DB Dir: " + str(os.path.dirname(self.cfg.db_file)) ], None
            else:
                if self.cfg.tracking:
                    self.log.track(3, "DB Dir Exists: " + str(os.path.dirname(self.cfg.db_file)), True)

            #-----------------------------------------------------------
            # Open a first connection with the DB (creates the DB file).
            try:        
                self.dbr = sqlite3.connect(self.cfg.db_file)
                if self.cfg.tracking:
                    self.log.track(3, "DB Connection Made.", True)
            except:
                if self.cfg.tracking:
                    self.log.errtrack("DB103", "Can't make DB Connection: " + str(self.cfg.db_file), True)
                return [ False, "DB103", "Can't make DB Connection: " + str(self.cfg.db_file) ], None

            #-----------------------------------------------------------
            # Get a cursor object for upcoming Table and Index creation
            # activity.
            try:
                cursor = self.dbr.cursor()
                if self.cfg.tracking:
                    self.log.track(3, "Got DB Cursor.", True)
            except:
                if self.cfg.tracking:
                    self.log.errtrack("DB104", "Failed to Get DB Cursor.")
                return [ False, "DB104", "Failed to Get DB Cursor." ], None

            #-----------------------------------------------------------
            # Create the 'admin' Table.
            try:
                cursor.execute("""CREATE TABLE admin
                    (machine TINYINT,
                    debugging TINYINT,
                    logging TINYINT,
                    tracking TINYINT,
                    timing TINYINT,
                    locking TINYINT,
                    type TEXT,
                    host TEXT,
                    port SMALLINT,
                    protocol TINYINT,
                    packet_size INTEGER,
                    sys_status_file TEXT,
                    data_dir TEXT,
                    data_dir_path TEXT,
                    db_file TEXT,
                    log_dir TEXT,
                    br_log_name TEXT,             
                    br_log_file TEXT,             
                    bs_log_name TEXT,
                    bs_log_file TEXT,
                    wt_changed BOOLEAN,             
                    pipo_scor_wt DECIMAL,
                    pipo_qual_wt DECIMAL,
                    pipo_size_wt DECIMAL,
                    pipo_trig_wt DECIMAL,
                    pipo_time_wt DECIMAL,
                    purge_rating DECIMAL,
                    max_msg_size INTEGER,
                    reserved1 BLOB,
                    reserved2 BLOB,
                    reserved3 BLOB,
                    reserved4 BLOB) 
                    """)

                if self.cfg.tracking:
                    self.log.track(3, "Created the 'admin' Table.", True)
            except:
                if self.cfg.tracking:
                    self.log.errtrack("DB111", "Failed to Create the 'admin' Table.")
                return [ False, "DB111", "Failed to Create the 'admin' Table." ], None

            #-----------------------------------------------------------
            # Create the 'status' Table.
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

                if self.cfg.tracking:
                    self.log.track(3, "Created the 'status' Table.", True)
            except:
                if self.cfg.tracking:
                    self.log.errtrack("DB105", "Failed to Create the 'status' Table.")
                return [ False, "DB105", "Failed to Create the 'status' Table." ], None

            #-----------------------------------------------------------
            # Create the 'meta' Table.
            try:
                cursor.execute("""CREATE TABLE meta
                    (state TINYINT,
                    base BOOLEAN,
                    status BIGINT,
                    numerator DOUBLE PRECISION,
                    trigger INTEGER,
                    pipo DOUBLE PRECISION,
                    type TEXT,
                    instance SMALLINT,
                    timestamp DOUBLE PRECISION,
                    heading DOUBLE PRECISION,
                    quality TINYINT,
                    score INTEGER,
                    metafile STRING,
                    stdfile STRING,
                    chgfile STRING,
                    stdsize INTEGER,
                    chgsize INTEGER,
                    norm INTEGER,
                    std TEXT,
                    chg TEXT)
                    """)

                if self.cfg.tracking:
                    self.log.track(3, "Created the 'meta' Table.", True)
            except  Exception as e:
                enum = "DB106"
                emsg = "reset(): [" + str(e)  + "]"
                if self.cfg.tracking:
                    self.log.errtrack(str(enum), str(emsg))
                return [ False, str(enum), str(emsg) ], None

            #-----------------------------------------------------------
            # Instantiate the 'admin' Table.

            sql = "INSERT INTO admin VALUES (%i,%i,%i,%i,%i,%i,'%s','%s',%i,%i,%i,'%s','%s','%s','%s','%s','%s','%s','%s','%s',%i,%.15f,%.15f,%.15f,%.15f,%.15f,%.15f,%i,NULL,NULL,NULL,NULL)" % (
                int(self.cfg.machine),
                int(self.cfg.debugging),
                int(self.cfg.logging),
                int(self.cfg.tracking),
                int(self.cfg.timing),
                int(self.cfg.locking),
                str(self.cfg.type),
                str(self.cfg.host),
                int(self.cfg.port),
                int(self.cfg.protocol),
                int(self.cfg.packet_size),
                str(self.cfg.sys_status_file),
                str(self.cfg.data_dir),
                str(self.cfg.data_dir_path),
                str(self.cfg.db_file),
                str(self.cfg.log_dir),
                str(self.cfg.br_log_name),
                str(self.cfg.br_log_file),
                str(self.cfg.bs_log_name),
                str(self.cfg.bs_log_file),
                int(self.cfg.wt_changed),
                float(self.cfg.pipo_scor_wt),
                float(self.cfg.pipo_qual_wt),
                float(self.cfg.pipo_size_wt),
                float(self.cfg.pipo_trig_wt),
                float(self.cfg.pipo_time_wt),
                float(self.cfg.purge_rating),
                int(self.cfg.max_msg_size)
            )

            try:
                cursor.execute(str(sql))
                self.dbr.commit()
                if self.cfg.tracking:
                    self.log.track(3, "Instantiated the 'admin' Table.", True)
            except Exception as e:
                if self.cfg.tracking:
                    self.log.errtrack("DB116", "Failed to Instantiate the 'admin' Table:" + str(e))
                return [ False, "DB116", "Failed to Instantiate the 'admin' Table:" + str(e) ], None
 
            #-----------------------------------------------------------
            # Close the Database.
            try:
                self.dbr.close()
                if self.cfg.tracking:
                   self.log.track(3, "Closed the DB.", True)
                return True
            except:
                if self.cfg.tracking:
                    self.log.errtrack("DB107", "Failed to Close the DB.")
                return [ False, "DB107", "Failed to Close the DB." ], None

        except Exception as e:
            if self.cfg.tracking:
                self.log.errtrack("DB108", "EXCEPTION: [" + str(e) + "]")
                return [ False, "DB108", "EXCEPTION: [" + str(e) + "]" ], None
                

            