########################################################################
##
# Module: bothelp.py
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

import os
import sys
import time
import uuid
import json
import shutil

import regex

import botdefs
from pylocker import Locker
from pathlib import Path
import regex

v_bothelp = "bot71-20200601"


########################################################################
# Check the required directories for device/bot exist.
########################################################################


########################################################################
# Retrieve the device NUID and put into variables
########################################################################
def getDevId(_cfg, _log, _lev, _filename):
    if _cfg.tracking:
        _log.track(_lev, "Entering 'getdevid()' Module.", True)
        _log.track(_lev + 13, "_filename: " + str(_filename), True)
    try:
        with open(_filename, "r") as f:
            val_str = f.readline().strip()

    except Exception as e:
        _log.track(
            _lev + 13, "getdevid(): Cannot open device NUID file (" + str(e) + ")", True
        )
        val_str = "0000000000"
    val_bytes = val_str.encode("utf-8")
    remote_id_str = "N" + val_str
    return val_str, val_bytes, remote_id_str


########################################################################
# Create mandatory Bot directories
########################################################################


def create_nepi_dirs(_cfg, _log, _lev):
    nepi_home = "../.."
    bot_dirs = [
        "devinfo",
        "cfg",
        "cfg/bot",
        "db",
        "log",
        "lb/data",
        "lb/cfg",
        "lb/dt-msg",
        "lb/do-msg",
        "hb/clone/dt",
        "hb/clone/do",
    ]

    bot_dirs_exp = list()
    for i in bot_dirs:
        bot_dirs_exp += f"{nepi_home}/{i}"
    try:
        for i in bot_dirs_exp:
            os.makedirs(i, 0o777, exist_ok=True)
            _log.track(_lev + 13, f"create_nepi_dirs(): created directory {i}", True)
    except Exception as e:
        _log.track(
            _lev + 13, f"create_nepi_dirs(): unable to create directory {i}", True
        )


########################################################################
# Split Data File 'payload' file name into fields
########################################################################


def check_metadata_filename(_filename: str):  # TODO: Make into class
    """
    :param _filename: metadata filename without path
    :type _filename: str
    :param _type: type field in metadata file
    :type _type: str
    :param _instance: instance field in metadata file
    :type _instance: str
    :return: Status, FilenameComponents
    :rtype: bool, tuple
    """
    m = regex.match(r"^([a-zA-Z]{3})([0-9]+)\.([a-zA-Z]+)$", _filename)
    if m is None:
        return False, ()
    return True, m.groups()


########################################################################
# Retrieve a list of the Float's Data Folders; Sort and Reverse.
########################################################################


def getAllFolderNames(_cfg, _log, _lev, _dir, _sortflag, _revflag):
    if _cfg.tracking:
        _log.track(_lev, "Entering 'getAllFolderNames()' Module.", True)
        _log.track(_lev + 13, "_cfg: " + str(_cfg), True)
        _log.track(_lev + 13, "_log: " + str(_log), True)
        _log.track(_lev + 13, "_lev: " + str(_lev), True)
        _log.track(_lev + 13, "_dir: " + str(_dir), True)
        _log.track(_lev + 13, "_srt: " + str(_sortflag), True)
        _log.track(_lev + 13, "_rev: " + str(_revflag), True)

    try:
        if not os.path.isdir(_dir):
            enum = "BH001"
            emsg = "getAllFolderNames(): Argument Not a Directory."
            if _cfg.tracking:
                _log.errtrack(str(enum), str(emsg))
            return [False, str(enum), str(emsg)], []

        # all_dirs = os.listdir(dir)
        # all_dirs = filter(os.path.isdir, os.listdir(dir))
        # all_dirs = [d for d in os.listdir(dir) if os.path.isdir(d)]
        all_dirs = next(os.walk(_dir))[1]

        if _sortflag:
            if _cfg.tracking:
                _log.track(_lev + 1, "Sort Requested.", True)
            all_dirs.sort()

        if _revflag:
            if _cfg.tracking:
                _log.track(_lev + 1, "Reversal Requested.", True)
            all_dirs.reverse()

        if _cfg.tracking:
            _log.track(_lev + 1, "Got All Folders.", True)
            _log.track(_lev + 14, "Names: " + " ".join(all_dirs), True)
            _log.track(_lev + 1, "Returning.", True)

        return [True, None, None], all_dirs

    except Exception as e:
        enum = "BH002"
        emsg = "getAllFolderNames(): [" + str(e) + "]"
        if _cfg.tracking:
            _log.errtrack(str(enum), str(emsg))
        return [False, str(enum), str(emsg)], []


########################################################################
# Return List of All File Names in a Dir; Optional: Sort and Reverse.
########################################################################


def getAllFileNames(_cfg, _log, _lev, _dir, _sortflag, _revflag):
    if _cfg.tracking:
        _log.track(_lev, "Entering 'getAllFileNames()' Module.", True)
        _log.track(_lev + 13, "_cfg: " + str(_cfg), True)
        _log.track(_lev + 13, "_log: " + str(_log), True)
        _log.track(_lev + 13, "_lev: " + str(_lev), True)
        _log.track(_lev + 13, "_dir: " + str(_dir), True)
        _log.track(_lev + 13, "_srt: " + str(_sortflag), True)
        _log.track(_lev + 13, "_rev: " + str(_revflag), True)

    try:
        if not os.path.isdir(_dir):
            enum = "BH010"
            emsg = "getAllFileNames(): Not a Directory."
            if _cfg.tracking:
                _log.errtrack(str(enum), str(emsg))
            return [False, str(enum), str(emsg)], []

        all_files = os.listdir(_dir)

        if _sortflag:
            if _cfg.tracking:
                _log.track(_lev + 1, "Sort Requested.", True)
            all_files.sort()

        if _revflag:
            if _cfg.tracking:
                _log.track(_lev + 1, "Reversal Requested.", True)
            all_files.reverse()

        if _cfg.tracking:
            # print('\n'.join(map(str, all_files)))
            # print (*all_files, sep = "\n")  # Python3 method
            _log.track(_lev + 1, "Got All Files.", True)
            _log.track(_lev + 14, "Names: " + " ".join(all_files), True)
            _log.track(_lev + 1, "Returning.", True)

        return [True, None, None], all_files

    except Exception as e:
        enum = "BH011"
        emsg = "getAllFileNames(): [" + str(e) + "]"
        if _cfg.tracking:
            _log.errtrack(str(enum), str(emsg))
        return [False, str(enum), str(emsg)], []


########################################################################
# Read a File from the Float's SD drive; optional lock or JSON-parse.
########################################################################


def readFloatFile(_cfg, _log, _lev, _ffile, _lockflag, _jsonflag):
    if _cfg is not None and _cfg.tracking:
        _log.track(_lev, "Entering 'readFloatFile()' Module.", True)
        _log.track(_lev + 1, "_cfg:      " + str(_cfg), True)
        _log.track(_lev + 1, "_log:      " + str(_log), True)
        _log.track(_lev + 1, "_lev:      " + str(_lev), True)
        _log.track(_lev + 1, "_ffile:    " + str(_ffile), True)
        _log.track(_lev + 1, "_lockflag: " + str(_lockflag), True)
        _log.track(_lev + 1, "_jsonflag: " + str(_jsonflag), True)

    if botdefs.locking and _lockflag:
        if _cfg is not None and _cfg.tracking:
            _log.track(_lev + 1, "File Locking Requested.", True)
            _log.track(_lev + 1, "Create Locker Instance.", True)

        FL = Locker(filePath=_ffile, lockPass=botdefs.lockpass, mode="r")

        # Acquire the lock. Using 'with' will insure all cleanup without us
        # having to worry about it.
        with FL as r:
            # Get the results
            acquired, code, fd = r

            # Check if Lock is acquired and a reasonable File Descriptor has
            # been returned; 'None' will be returned in the 'fd' if the fd
            # hasn't been successfully created.  Not clear from File Locker
            # documentation if this 'double check' is actually redundant.
            if acquired and fd is not None:
                try:
                    ffdata = fd.read()
                    return [True, None, None], ffdata
                except Exception as e:
                    enum = "BH021"
                    emsg = "readFloatFile(): [" + str(e) + "]"
                    if _cfg is not None and _cfg.tracking:
                        _log.errtrack(str(enum), str(emsg))
                    return [False, str(enum), str(emsg)], None
            else:
                enum = "BH022"
                emsg = "readFloatFile(): Can't Access File."
                if _cfg is not None and _cfg.tracking:
                    _log.errtrack(str(enum), str(emsg))
                return [False, str(enum), str(emsg)], None
    else:
        if _cfg is not None and _cfg.tracking:
            _log.track(_lev + 1, "File Locking NOT Requested.", True)
        try:
            if _cfg is not None and _cfg.tracking:
                _log.track(_lev + 1, "Open File.", True)
            fd = open(_ffile, "r")

            if _cfg is not None and _cfg.tracking:
                _log.track(_lev + 1, "Read Data.", True)
            ffdata = fd.read()
            if _cfg is not None and _cfg.tracking:
                _log.track(_lev + 1, "Data: " + str(ffdata), True)

            if _jsonflag:
                if _cfg is not None and _cfg.tracking:
                    _log.track(_lev + 1, "JSON Loads Requested.", True)
                ffdata = json.loads(ffdata)
                if _cfg is not None and _cfg.tracking:
                    _log.track(_lev + 1, "JSON: " + str(ffdata), True)

            if _cfg is not None and _cfg.tracking:
                _log.track(_lev + 1, "Return.", True)
            return [True, None, None], ffdata
        except Exception as e:
            enum = "BH021"
            emsg = "readFloatFile(): [" + str(e) + "]"
            if _cfg is not None and _cfg.tracking:
                _log.errtrack(str(enum), str(emsg))
            return [False, str(enum), str(emsg)], None


########################################################################
# Write a File to the Float's SD drive.
########################################################################


def writeFloatFile(_cfg, _log, _lev, _clear, _ffile, _fdata):
    if _cfg.tracking:
        _log.track(_lev, "Entering 'writeFloatFile()' Module.", True)
        _log.track(_lev + 13, "_cfg:   " + str(_cfg), True)
        _log.track(_lev + 13, "_log:   " + str(_log), True)
        _log.track(_lev + 13, "_lev:   " + str(_lev), True)
        _log.track(_lev + 13, "_clear: " + str(_clear), True)
        _log.track(_lev + 13, "_ffile: " + str(_ffile), True)
        _log.track(_lev + 13, "_fdata: " + str(_fdata), True)

    # -------------------------------------------------------------------
    # If 'clear' is True and file exists, delete it.
    # -------------------------------------------------------------------
    if _clear:
        if _cfg.tracking:
            _log.track(_lev + 1, "File 'clear' Requested.", True)

        if os.path.isfile(_ffile):
            if _cfg.tracking:
                _log.track(_lev + 2, "File Exists; Remove it.", True)
            try:
                os.remove(_ffile)
                _log.track(_lev + 2, "Remove File Successful.", True)
            except Exception as e:
                enum = "BH031"
                emsg = "writeFloatFile(): [" + str(e) + "]"
                if _cfg.tracking:
                    _log.track(_lev + 2, "ERROR: " + str(enum) + ": " + str(emsg))
                return [False, str(enum), str(emsg)]
        else:
            if _cfg.tracking:
                _log.track(_lev + 1, "File does not exist; OK, Continue.", True)
    else:
        if _cfg.tracking:
            _log.track(_lev + 1, "File 'clear' NOT Requested.", True)

    # -------------------------------------------------------------------
    # Check Directory Path, Create it if Necessary.
    # -------------------------------------------------------------------
    try:
        dir = os.path.dirname(_ffile)
        if _cfg.tracking:
            _log.track(_lev + 1, "Check File Directory.", True)
            _log.track(_lev + 2, "Dir: " + str(dir), True)

        if not os.path.isdir(str(dir)):
            if _cfg.tracking:
                _log.track(_lev + 2, "File Directory doesn't exist; Create It.", True)
            os.makedirs(str(dir))
            if _cfg.tracking:
                _log.track(_lev + 2, "Created Dir: " + str(dir), True)
        else:
            if _cfg.tracking:
                _log.track(_lev + 2, "DB Directory Already Exists.", True)

    except Exception as e:
        enum = "BH032"
        emsg = "writeFloatFile(): [" + str(e) + "]"
        if _cfg.tracking:
            _log.track(_lev + 2, "ERROR: " + str(enum) + ": " + str(emsg))
        return [False, str(enum), str(emsg)]

    # -------------------------------------------------------------------
    # Open File for Append; Write Information.
    # -------------------------------------------------------------------
    if _cfg.tracking:
        _log.track(_lev + 1, "Open/Append File.", True)

    try:
        with open(_ffile, "a") as f:
            f.write(_fdata)

        if _cfg.tracking:
            _log.track(_lev + 2, "File Append Complete.", True)

    except Exception as e:
        enum = "BH033"
        emsg = "writeFloatFile(): [" + str(e) + "]"
        if _cfg.tracking:
            _log.track(_lev + 2, "ERROR: " + str(enum) + ": " + str(emsg))
        return [False, str(enum), str(emsg)]

    # -------------------------------------------------------------------
    # Close File to be Safe.
    # -------------------------------------------------------------------
    if _cfg.tracking:
        _log.track(_lev + 1, "Close File.", True)

    if f.closed:
        if _cfg.tracking:
            _log.track(_lev + 2, "File Already Closed.", True)
    else:
        f.close()
        if _cfg.tracking:
            _log.track(_lev + 2, "File Closed.", True)

    return [True, None, None]


########################################################################
# Trigger Score Lookup Table.
########################################################################

# Needs to be addressed more robustly; must be configurable.  For now,
# we're using a simplified default return value.


def triggerScoreLookup(_cfg, _log, _lev, status_json):
    if _cfg.tracking:
        _log.track(_lev, "Entering 'triggerScoreLookup()' Module.", True)
        _log.track(_lev + 1, "_cfg:  " + str(_cfg), True)
        _log.track(_lev + 1, "_log:  " + str(_log), True)
        _log.track(_lev + 1, "_lev:  " + str(_lev), True)
        _log.track(_lev + 13, "_json: " + str(status_json), True)

    try:
        wake_event_type = int(status_json.get("wake_event_type", 0))
        wake_event_id = int(status_json.get("wake_event_id", 1.0))
    except Exception as e:
        enum = "BH041"
        emsg = "triggerScoreLookup(): [" + str(e) + "]"
        if _cfg.tracking:
            _log.track(_lev + 2, str(enum), str(emsg))

        wake_event_type = 0
        wake_event_id = 1.0

    if wake_event_type == 0:
        trigger = 1.0
    else:
        trigger = 0.5

    return trigger, wake_event_type, wake_event_id


########################################################################
# Reset Config File Value.
########################################################################


def resetCfgValue(cfg, log, lev, key, val):
    if cfg.tracking:
        log.track(lev, "Entering resetCfgValue() Module.", True)
        log.track(lev + 1, "lev: " + str(lev), True)
        log.track(lev + 1, "key: " + str(key), True)
        log.track(lev + 1, "val: " + str(val), True)

    try:
        with open(cfg.cfgfile, "r") as cfile:
            cdata = json.load(cfile)

        cdata[str(key)] = val

        with open(cfg.cfgfile, "w") as cfile:
            json.dump(cdata, cfile, indent=4)
    except Exception as e:
        if log.tracking:
            log.errtrack("CFG003", "Update failure: " + str(e))
        return [False, "CFG003", "Update failure: " + str(e)]

    return [True, None, None]


########################################################################
# Delete Data Product Folder.
########################################################################


def deleteFolder(_cfg, _log, _lev, _dpf):
    if _cfg.tracking:
        _log.track(_lev, "Entering deleteFolder() Module.", True)
        _log.track(_lev + 13, "_cfg: " + str(_cfg), True)
        _log.track(_lev + 13, "_log: " + str(_log), True)
        _log.track(_lev + 13, "_lev: " + str(_lev), True)
        _log.track(_lev + 13, "_dpf: " + str(_dpf), True)
        _log.track(_lev + 13, "Remove Entire Folder Tree.", True)

    try:
        shutil.rmtree(_dpf)
        if _cfg.tracking:
            _log.track(_lev + 1, "Success: Folder Removed.", True)
    except Exception as e:
        if _cfg.tracking:
            _log.track(_lev + 1, "ERROR: [" + str(e) + "]", True)


########################################################################
# Delete Data Product.
########################################################################


def deleteDataProduct(_cfg, _log, _lev, _dp):
    if _cfg.tracking:
        _log.track(_lev, "Entering deleteDataProduct() Module.", True)
        _log.track(_lev + 7, "_cfg: " + str(_cfg), True)
        _log.track(_lev + 7, "_log: " + str(_log), True)
        _log.track(_lev + 7, "_lev: " + str(_lev), True)
        _log.track(_lev + 7, "_dp:  " + str(_dp), True)
        _log.track(_lev + 1, "Delete All Data Product File.", True)

    try:
        std = _dp.replace("meta", "std")
    except:
        std = None

    try:
        chg = _dp.replace("meta", "change")
    except:
        chg = None

    try:
        if _dp:
            if _cfg.tracking:
                _log.track(_lev + 2, "Remove: " + str(_dp), True)
            if os.path.isfile(_dp):
                os.remove(_dp)
            if _cfg.tracking:
                _log.track(_lev + 3, "Done.", True)
        else:
            if _cfg.tracking:
                _log.track(_lev + 3, "None.", True)
    except:
        if _cfg.tracking:
            _log.track(_lev + 3, "Ignored.", True)

    try:
        if std:
            if _cfg.tracking:
                _log.track(_lev + 2, "Remove: " + str(std), True)
            if os.path.isfile(std):
                os.remove(std)
            if _cfg.tracking:
                _log.track(_lev + 3, "Done.", True)
        else:
            if _cfg.tracking:
                _log.track(_lev + 3, "None.", True)
    except:
        if _cfg.tracking:
            _log.track(_lev + 3, "Ignored.", True)

    try:
        if chg:
            if _cfg.tracking:
                _log.track(_lev + 2, "Remove: " + str(chg), True)
            if os.path.isfile(chg):
                os.remove(chg)
            if _cfg.tracking:
                _log.track(_lev + 3, "Done.", True)
        else:
            if _cfg.tracking:
                _log.track(_lev + 3, "None.", True)
    except:
        if _cfg.tracking:
            _log.track(_lev + 3, "Ignored.", True)
