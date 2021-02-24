########################################################################
##
# Module: botreports.py
# --------------------
##
# (c) Copyright 2020 by Numurus LLC
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
import re
from collections import OrderedDict
from pathlib import Path
import json

from botdefs import nepi_home


class LbConnItem:
    def __init__(self, _comms_type, _status):
        self.comms_type = _comms_type
        self.status = _status
        self.timestart = None
        self.timestop = None
        self.msgsent = 0
        self.pktsent = 0
        self.statsent = 0
        self.datasent = 0
        self.gensent = 0
        self.msgrecv = 0
        self.cfgrecv = 0
        self.genrecv = 0
        self.errorlist = []

    def update_timestart(self):
        dt = datetime.datetime.utcnow()
        self.timestart = datetime.datetime.isoformat(dt)
        pass

    def update_timestop(self):
        dt = datetime.datetime.utcnow()
        self.timestop = datetime.datetime.isoformat(dt)
        pass

    def update_msgsent(self, amt):
        self.msgsent += amt
        return

    def update_pktsent(self, amt):
        self.pktsent += amt
        return

    def update_statsent(self, amt):
        self.statsent += amt
        return

    def update_datasent(self, amt):
        self.datasent += amt
        return

    def update_gensent(self, amt):
        self.gensent += amt
        return

    def update_msgrecv(self, amt):
        self.msgrecv += amt
        return

    def update_cfgrecv(self, amt):
        self.cfgrecv += amt
        return

    def update_genrecv(self, amt):
        self.genrecv += amt
        return

    def generate_stats(self):

        try:
            d = OrderedDict()
            d["comms_type"] = self.comms_type
            d["status"] = self.status
            d["timestart"] = self.timestart
            d["timestop"] = self.timestop
            d["msgsent"] = self.msgsent
            d["pktsent"] = self.pktsent
            d["statsent"] = self.statsent
            d["datasent"] = self.datasent
            d["gensent"] = self.gensent
            d["msgrecv"] = self.msgrecv
            d["cfgrecv"] = self.cfgrecv
            d["genrecv"] = self.genrecv
            d["errors"] = self.errorlist

            return d
        except:
            return None


class LbRpt:
    def __init__(self, _outfname, _lb_conn_list):
        self.outfname = os.path.abspath(nepi_home + '/log/' + _outfname)
        self.lb_conn_list = _lb_conn_list

    def create_lb_report(self):

        # try:
        #     os.chdir(os.path.abspath(nepi_home + '/' + 'src/bot'))
        # except:
        #     pass

        d = OrderedDict()
        d["connections"] = []
        for i in self.lb_conn_list:
            if i:
                info = i.generate_stats()
                d["connections"].append(info)

        if not len(d["connections"]):
            return

        try:
            x = Path.cwd()
            print(f"{x=}")

            result = json.dumps(d, indent=2)

            with open(self.outfname, 'w') as f:
                f.writelines(result)

            return
        except:
            return


class HbConnItem:

    def __init__(self, _comms_type, _status, _dtype):
        self.comms_type = _comms_type
        self.status = _status
        self.dtype = _dtype
        self.timestart = None
        self.timestop = None
        self.datasent_kB = 0
        self.datarecv_kB = 0
        self.numdirs = 0
        self.numfiles = 0
        self.errorlist = []
        self.warninglist = []

    def parse_rsync_file(self, _filename):
        self.filename = _filename

        # check the file exists
        if not Path(self.filename).exists():
            return None

        # read lines into a list
        try:
            with open(self.filename) as f:
                lines = f.readlines()
        except:
            return None
        try:
            # gets rsync timestamp from log file
            pat_ts = re.compile(r"""
            (\d{4})/(\d{2})/(\d{2}) # date
            \s # one whitespace char
            (\d{2}:\d{2}:\d{2}) # time
            """, re.VERBOSE)

            # get first timestamp in log file for timestart
            m = re.match(pat_ts, lines[0])
            if not m:
                self.timestart = ""
            else:
                self.timestart = f"{m.group(1)}-{m.group(2)}-{m.group(3)}T{m.group(4)}.000000"

            # get last timestamp in log file for timestop
            m = re.match(pat_ts, lines[-1])
            if not m:
                self.timestop = ""
            else:
                self.timestop = f"{m.group(1)}-{m.group(2)}-{m.group(3)}T{m.group(4)}.000000"

            pat_warning = re.compile(r"""
            warning:* (.*)
            """, re.VERBOSE | re.IGNORECASE)

            pat_error = re.compile(r"""
            error:* (.*)
            """, re.VERBOSE | re.IGNORECASE)

            pat_finfo = re.compile(r"""
            number of files:.*reg: ([\d,]*), dir: ([\d]*)
            """, re.VERBOSE | re.IGNORECASE)

            # go through file and get information we need for report
            for line in lines:
                m = re.search(r'Total bytes sent: ([\d,]+)', line, flags=re.IGNORECASE)
                if m:
                    mstr = m.group(1).replace(',', '')
                    self.datasent_kB = int(int(mstr) / 1024) + 1
                    continue
                m = re.search(r'Total bytes received: ([\d,]+)', line, flags=re.IGNORECASE)
                if m:
                    mstr = m.group(1).replace(',', '')
                    self.datarecv_kB = int(int(mstr) / 1024) + 1
                    continue
                m = re.search(r'number of files:.*reg:[^\d]*([\d,]+).*dir:.*([\d,]+)', line, flags=re.IGNORECASE)
                if m:
                    mstr = m.group(1).replace(',', '')
                    self.numfiles = int(mstr)
                    mstr = m.group(2).replace(',', '')
                    self.numdirs = int(mstr)
                    continue
                m = re.search(pat_error, line)
                if m:
                    self.errorlist.append(m.group(1))
                    continue
                m = re.search(pat_warning, line)
                if m:
                    self.warninglist.append(m.group(1))
                    continue

            # create an ordered dictionary to dump out in proper form

            d = OrderedDict()
            d["comms_type"] = self.comms_type
            d['status'] = self.status
            d['dtype'] = self.dtype
            d['timestart'] = self.timestart
            d['timestop'] = self.timestop
            d['datasent_kB'] = self.datasent_kB
            d['datarecv_kB'] = self.datarecv_kB
            d['numdirs'] = self.numdirs
            d['numfiles'] = self.numfiles
            d['errors'] = self.errorlist
            d['warnings'] = self.warninglist

            return d
        except:
            return None


class HbRpt:
    def __init__(self, _outfile):
        self.filename = os.path.abspath(nepi_home + '/log/' + _outfile)

    def create_hb_report(self):

        # aggregate do data
        do_data = HbConnItem('hb_ip', 'success', 'do')
        sw_data = HbConnItem('hb_ip', 'success', 'sw')
        do_infile=os.path.abspath(nepi_home + '/log/bot_do_transfer.log')
        sw_infile = os.path.abspath(nepi_home + '/log/bot_sw_transfer.log')
        do_stats = do_data.parse_rsync_file(do_infile)
        sw_stats = sw_data.parse_rsync_file(sw_infile)

        d = OrderedDict()
        d["connections"] = []
        # d["connections"]= [do_stats, sw_stats]
        if do_data:
            d["connections"].append(do_stats)
        if sw_data:
            d["connections"].append(sw_stats)

        result = json.dumps(d, indent=2)

        with open(self.filename, 'w') as f:
            f.writelines(result)

        return
