########################################################################
##
##  Module: bothelp.py
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
##  Revision:   1.3 2019/02/19  09:10:00
##  Comment:    Added Config File value restting functionality.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
##  Revision:   1.2 2019/02/13  15:30:00
##  Comment:    Added Trigger Score lookup functionality.
##  Developer:  John benJohn, Leonardo, New Jersey
##  Platform:   Ubuntu 16.05; Python 2.7.12
##
##  Revision:   1.1 2019/02/04  09:10:00
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
import botdefs
from pylocker import Locker

########################################################################
# Retrieve a list of the Float's Data Folders; Sort and Reverse.
########################################################################

def getAllFolderNames(cfg, log, lev, dir, sortem, revem):
    if cfg.tracking :
        log.track(lev, "Entering 'getAllFolderNames()' Module.", True)
        log.track(lev+1, "Dir: " + str(dir), True)

        if not os.path.isdir(dir):
            return [ False, "AF001", "NOT A DIRECTORY" ], None

    try:
        #all_dirs = os.listdir(dir)

        #all_dirs = filter(os.path.isdir, os.listdir(dir))
        #all_dirs =  [d for d in os.listdir(dir) if os.path.isdir(d)] 
        all_dirs =  next(os.walk(dir))[1]    

        if sortem:
            if cfg.tracking:
                log.track(lev, "Sort Requested.", True)
            #all_dirs.sort()

        if revem:
            if cfg.tracking:
                log.track(lev, "Reversal Requested.", True)
            #all_dirs.reverse()

        if cfg.tracking:
            log.track(lev+5, "All Folders: " + str(all_dirs), True)

        return [True, None, None], all_dirs

    except Exception as e:
        if cfg.tracking:
            log.errtrack("AF002", "Get All Folders FAILED.")
            log.errtrack("AF002", "EXCEPTION: [" + str(e) + "]")
        
        return [ False, "AF002", "EXCEPTION: [" + str(e) + "]" ], None

########################################################################
# Return List of All File Names in a Dir; Optional: Sort and Reverse.
########################################################################

def getAllFileNames(cfg, log, lev, dir, sortem, revem):
    if cfg.tracking :
        log.track(lev, "Entering 'getAllFileNames()' Module.", True)
        log.track(lev+1, "Dir: " + str(dir), True)

    try:
        if not os.path.isdir(dir):
            return [ False, "AFN001", "NOT A DIRECTORY" ], None

        all_files = os.listdir(dir)
        if sortem:
            if cfg.tracking:
                log.track(lev, "Sort Requested.", True)
            all_files.sort()

        if revem:
            if cfg.tracking:
                log.track(lev, "Reversal Requested.", True)
            all_files.reverse()

        if cfg.tracking:
            log.track(lev+5, "All File Names: " + str(all_files), True)
            #print('\n'.join(map(str, all_files)))
            #print (*all_files, sep = "\n")  # Python3 method

        return [True, None, None], all_files

    except Exception as e:
        if cfg.tracking:
            log.errtrack("AFN002", "Get All Files FAILED.")
            log.errtrack("AFN002", "EXCEPTION: [" + str(e) + "]")

        return [ False, "AFN002", "EXCEPTION: [" + str(e) + "]" ], None

########################################################################
##  Read a File from the Float's SD drive; optional lock or JSON-parse.
########################################################################

def readFloatFile(ffile, lockflag, jsonflag):

    ffdata = None
    acquired = False
    # Create Locker instance.
    if botdefs.locking and lockflag:
        FL = Locker(filePath=ffile, lockPass=botdefs.lockpass, mode='r')

        # Acquire the lock. Using 'with' will insure all cleanup without us
        # having to worry about it.
        with FL as r:
            # Get the results
            acquired, code, fd = r

            # Check if Lock is acquired and a reasonable File Descriptor has
            # been returned; 'None' will be returned in the 'fd' if the fd
            # hasn't been successfully created.  Not clear from File Locker
            # documentation if this 'double check' is actually redundant.
            ffdata = None
            if acquired and fd is not None:
                try:
                    ffdata = fd.read()
                except:
                    acquired = False
                    ffdata = None
            else:
                acquired = False
                ffdata = None
    else:
        try:
            fd = open(ffile,'r')
            ffdata = fd.read()
            if jsonflag:
                ffdata = json.loads(ffdata)
            acquired = True
        except:
            acquired = False
            ffdata = None

    return acquired, ffdata

########################################################################
##  Trigger Score Lookup Table.
########################################################################

# Needs to be addressed more robustly; must be configurable.  For now,
# we're using a simplified default return value.

def triggerScoreLookup(status_json):
    wake_event_type = str(status_json["wake_event_type"])
    wake_event_id = str(status_json["wake_event_id"])

    trigger = 0.5
    if wake_event_type == 0:
        trigger = 1.0
    else:
        trigger = 0.5

    return trigger, wake_event_type, wake_event_id
    
########################################################################
##  Reset Config File Value.
########################################################################

def resetCfgValue(cfg, log, lev, key, val):
    if cfg.tracking:
        log.track(lev, "Entering resetValue() Module.", True)
        log.track(lev+1, "lev: " + str(lev), True)
        log.track(lev+1, "key: " + str(key), True)
        log.track(lev+1, "val: " + str(val), True)
    
    try:
        with open(cfg.cfgfile, 'r') as cfile:
            cdata = json.load(cfile)

        cdata[str(key)] = str(val)

        with open(cfg.cfgfile, 'w') as cfile:
            json.dump(cdata, cfile, indent=4)
    except Exception as e:
        if log.tracking:
            log.errtrack("CFG003", "Update failure: " + str(e))
        return [ False, "CFG003", "Update failure: " + str(e) ]

    return [ True, None, None ]

