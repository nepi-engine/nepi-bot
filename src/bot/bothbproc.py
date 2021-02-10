########################################################################
##
# Module: bothbproc.py
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
from threading import Timer

import botdefs
import bothelp
from datetime import datetime
from pathlib import Path
import os
import subprocess


class HbProc(object):
    def __init__(self, _cfg, _log, _lev, _dev_id_str, _nepi_args):
        # global gen_msg_contents

        self.cfg = _cfg
        self.log = _log
        self.lev = _lev
        self.dev_id_str = _dev_id_str
        self.nepi_args = _nepi_args
        self.gen_msg_contents = ""
        self.original_dir = Path.cwd()
        self.hb_dir = os.path.abspath(botdefs.bot_hb_dir)
        self.ssh_key_file = os.path.abspath(botdefs.bot_devsshkeys_file)
        self.botlock_fname = '.botlock'     # written on server in home directory while bot is busy on server.
        self.server_do_dir = '.'   # where the dirs and files under nepi_home/hb/do go.
        self.server_log_dir = './NEPI-Logs'

        if self.cfg.tracking:
            self.log.track(self.lev, "Created HbProc Class Object.", True)

    # function to set flag when thread timer goes off
    def hb_early_terminate(self):
        if bothelp.exit_event_hb.is_set():
            if self.cfg.tracking:
                self.log.track(self.lev, "HB820: Received TIMEOUT command. Terminating early.", True)
                self.log.track(
                    self.lev, f"Current working directory is: {self.original_dir}.", True
                )
        bothelp.exit_event_hb.set()
        return


    # double check hb directory structure on nepibot and server
    def check_hb_dirs(self):

        if bothelp.exit_event_hb.is_set():
            if self.cfg.tracking:
                self.log.track(self.lev, "HB810: Received STOP command. Terminating early.", True)
                self.log.track(
                    self.lev, f"Current working directory is: {self.original_dir}.", True
                )
            return True
        if self.cfg.tracking:
            self.log.track(self.lev, "Entering check_hb_dirs() method.", True)
            self.log.track(
                self.lev, f"Current working directory is: {self.original_dir}.", True
            )
        self.gen_msg_contents += "Entering check_hb_dirs() method.\n"
        os.chdir(self.hb_dir)
        if self.cfg.tracking:
            self.log.track(self.lev, f"Changed directory to: {self.hb_dir}.", True)
        self.gen_msg_contents += f"Changed directory to: {self.hb_dir}.\n"
        try:
            self.gen_msg_contents += f"Created do dt logs directories.\n"
            Path(f"{self.cfg.hb_dir_outgoing}").mkdir(
                mode=0o755, parents=True, exist_ok=True
            )
            Path(f"{self.cfg.hb_dir_incoming}").mkdir(
                mode=0o755, parents=True, exist_ok=True
            )
            Path(f"log").mkdir(mode=0o755, parents=True, exist_ok=True)
            self.gen_msg_contents += f"Created {self.cfg.hb_dir_outgoing} {self.cfg.hb_dir_incoming} log directories.\n"
            if self.cfg.tracking:
                self.log.track(
                    self.lev,
                    f"Created {self.cfg.hb_dir_outgoing} {self.cfg.hb_dir_incoming} log dirs.",
                    True,
                )
        except Exception as e:
            self.gen_msg_contents += (
                f"Unable to create local ${self.hb_dir}/<do|dt|logs> directories.\n"
            )
            if self.cfg.tracking:
                self.log.track(
                    self.lev,
                    f"Unable to create local {self.cfg.hb_dir_outgoing} {self.cfg.hb_dir_incoming} logs directories.",
                    True,
                )
        if bothelp.exit_event_hb.is_set():
            if self.cfg.tracking:
                self.log.track(self.lev, "HB811: Received STOP command. Terminating early.", True)
                self.log.track(
                    self.lev, f"Current working directory is: {self.original_dir}.", True
                )
            return True

        # write a lock file on server so it knows bot is busy
        args_sshcmd = [
            "ssh",
            "-p",
            f"{self.cfg.hb_ip.port}",
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "GlobalKnownHostsFile=/dev/null",
            "-o",
            "UserKnownHostsFile=/dev/null",
            "-i",
            f"{self.ssh_key_file}",
            f"{self.dev_id_str}@{self.cfg.hb_ip.host}",
            f"echo > {self.botlock_fname}",
        ]
        self.gen_msg_contents += f"Built ssh command.\n\t{args_sshcmd}\n"
        try:
            ssh_cmd = subprocess.run(
                args_sshcmd,
                universal_newlines=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            if ssh_cmd.returncode != 0:
                if self.cfg.tracking:
                    self.log.track(
                        self.lev, f"Unable to create {self.botlock_fname} on server.", True
                    )
                self.gen_msg_contents += (
                    f"ssh command returned error code {ssh_cmd.returncode}\n"
                )
            if self.cfg.tracking:
                self.log.track(
                    self.lev,
                    f"Created {self.botlock_fname} on server.",
                    True,
                )
        except Exception as e:
            if self.cfg.tracking:
                self.log.track(
                    self.lev,
                    f"Unable to create {self.botlock_fname} on server. {ssh_cmd.returncode}.",
                    True,
                )
            return False

        # check directories on server
        args_sshcmd = [
            "ssh",
            "-p",
            f"{self.cfg.hb_ip.port}",
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "GlobalKnownHostsFile=/dev/null",
            "-o",
            "UserKnownHostsFile=/dev/null",
            "-i",
            f"{self.ssh_key_file}",
            f"{self.dev_id_str}@{self.cfg.hb_ip.host}",
            f"mkdir -p {self.cfg.hb_dir_outgoing} {self.cfg.hb_dir_incoming} log",
            ]
        self.gen_msg_contents += f"Built ssh command.\n\t{args_sshcmd}\n"
        try:
            ssh_cmd = subprocess.run(
                args_sshcmd,
                universal_newlines=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            if ssh_cmd.returncode != 0:
                if self.cfg.tracking:
                    self.log.track(
                        self.lev, "Unable to create hb directories on server.", True
                    )
                self.gen_msg_contents += (
                    f"ssh command returned error code {ssh_cmd.returncode}\n"
                )
            if self.cfg.tracking:
                self.log.track(
                    self.lev,
                    f"Created {self.cfg.hb_dir_outgoing} {self.cfg.hb_dir_incoming} log dirs on server.",
                    True,
                )
        except Exception as e:
            if self.cfg.tracking:
                self.log.track(
                    self.lev,
                    f"Unable to create {self.cfg.hb_dir_outgoing} {self.cfg.hb_dir_incoming} log dirs on server {ssh_cmd.returncode}.",
                    True,
                )
            return False
        return True

    # transfer files from nepibot to server
    def transfer_files(self):

        if bothelp.exit_event_hb.is_set():
            if self.cfg.tracking:
                self.log.track(self.lev, "HB812: Received STOP command. Terminating early.", True)
                self.log.track(
                    self.lev, f"Current working directory is: {self.original_dir}.", True
                )
            return True

        datetimestr = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        os.chdir(f"{self.hb_dir}")
        if self.cfg.tracking:
            self.log.track(self.lev, f"Changed directory to: {self.hb_dir}.", True)

        # transfer do files from bot to server

        args_rsync = [
            "rsync",
            f"--log-file=log/do_temp.log",
            "--remove-source-files",
            "--stats",
            "-rIvvatiRzte",
            f"ssh -p {self.cfg.hb_ip.port} -o StrictHostKeyChecking=no -o GlobalKnownHostsFile=/dev/null -o UserKnownHostsFile=/dev/null -i {self.ssh_key_file}",
            f"{self.cfg.hb_dir_outgoing}",
            f"{self.dev_id_str}@{self.cfg.hb_ip.host}:~", #{self.cfg.hb_dir_outgoing}",
        ]

        rsync_cmd = subprocess.run(
            args_rsync,
            universal_newlines=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if rsync_cmd.returncode != 0:
            self.gen_msg_contents += f"Error encountered executing rsync to server. Return code = {rsync_cmd.returncode}"
            if self.cfg.tracking:
                self.log.track(
                    self.lev,
                    f"Error encountered executing rsync to server. Return code = {rsync_cmd.returncode}",
                    True,
                )

        # transfer do log file from bot to server

        args_rsync = [
            "rsync",
            f"--include=log/do_temp.log",
            "-rvvatiRzte",
            f"ssh -p {self.cfg.hb_ip.port} -o StrictHostKeyChecking=no -o GlobalKnownHostsFile=/dev/null -o UserKnownHostsFile=/dev/null -i {self.ssh_key_file}",
            f"log",
            f"{self.dev_id_str}@{self.cfg.hb_ip.host}:~", #"/{self.cfg.hb_dir_outgoing}",
        ]

        rsync_cmd = subprocess.run(
            args_rsync,
            universal_newlines=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if rsync_cmd.returncode != 0:
            self.gen_msg_contents += f"Error encountered executing rsync of log file to server. Return code = {rsync_cmd.returncode}"
            if self.cfg.tracking:
                self.log.track(
                    self.lev,
                    f"Error encountered executing rsync of log file to server. Return code = {rsync_cmd.returncode}",
                    True,
                )
        if bothelp.exit_event_hb.is_set():
            if self.cfg.tracking:
                self.log.track(self.lev, "HB813: Received STOP command. Terminating early.", True)
                self.log.track(
                    self.lev, f"Current working directory is: {self.original_dir}.", True
                )
            return True

        # transfer dt files from server to bot

        args_rsync = [
            "rsync",
            f"--log-file=log/dt_temp.log",
            "--remove-source-files",
            "--stats",
            "-rIvvatiRzte",
            f"ssh -p {self.cfg.hb_ip.port} -o StrictHostKeyChecking=no -o GlobalKnownHostsFile=/dev/null -o UserKnownHostsFile=/dev/null -i {self.ssh_key_file}",
            f"{self.dev_id_str}@{self.cfg.hb_ip.host}:{self.cfg.hb_dir_incoming}",
            f"dt",
        ]
        # AGV
        rsync_cmd = subprocess.run(
            args_rsync,
            universal_newlines=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if rsync_cmd.returncode != 0:
            self.gen_msg_contents += f"Error encountered executing rsync to server. Return code = {rsync_cmd.returncode}"
            if self.cfg.tracking:
                self.log.track(
                    self.lev,
                    f"Error encountered executing rsync to server. Return code = {rsync_cmd.returncode}",
                    True,
                )
        if bothelp.exit_event_hb.is_set():
            if self.cfg.tracking:
                self.log.track(self.lev, "HB814: Received STOP command. Terminating early.", True)
                self.log.track(
                    self.lev, f"Current working directory is: {self.original_dir}.", True
                )
            return True

        # transfer dt log file from bot to server

        args_rsync = [
            "rsync",
            f"--include=log/dt_temp.log",
            "-rvvatiRzte",
            f"ssh -p {self.cfg.hb_ip.port} -o StrictHostKeyChecking=no -o GlobalKnownHostsFile=/dev/null -o UserKnownHostsFile=/dev/null -i {self.ssh_key_file}",
            f"log",
            f"{self.dev_id_str}@{self.cfg.hb_ip.host}:~",
        ]

        rsync_cmd = subprocess.run(
            args_rsync,
            universal_newlines=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if rsync_cmd.returncode != 0:
            self.gen_msg_contents += f"Error encountered executing rsync of log file to server. Return code = {rsync_cmd.returncode}"
            if self.cfg.tracking:
                self.log.track(
                    self.lev,
                    f"Error encountered executing rsync of log file to server. Return code = {rsync_cmd.returncode}",
                    True,
                )
        if bothelp.exit_event_hb.is_set():
            if self.cfg.tracking:
                self.log.track(self.lev, "HB815: Received STOP command. Terminating early.", True)
                self.log.track(
                    self.lev, f"Current working directory is: {self.original_dir}.", True
                )
            return True

        # mv dt subdirectory rsync creates into dt.
        completed = subprocess.run(
            ["mv -f dt/hb/dt/* hb/dt"],
            shell=True,
            universal_newlines=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        completed = subprocess.run(
            ["rm -fr dt/hb"],
            shell=True,
            universal_newlines=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        completed = subprocess.run(
            ["find hb -empty -type d -delete"],
            shell=True,
            universal_newlines=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        # remove the lock file on server so it knows bot is done
        args_sshcmd = [
            "ssh",
            "-p",
            f"{self.cfg.hb_ip.port}",
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "GlobalKnownHostsFile=/dev/null",
            "-o",
            "UserKnownHostsFile=/dev/null",
            "-i",
            f"{self.ssh_key_file}",
            f"{self.dev_id_str}@{self.cfg.hb_ip.host}",
            f"rm -f {self.botlock_fname}",
        ]
        self.gen_msg_contents += f"Built ssh command.\n\t{args_sshcmd}\n"
        try:
            ssh_cmd = subprocess.run(
                args_sshcmd,
                universal_newlines=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            if ssh_cmd.returncode != 0:
                if self.cfg.tracking:
                    self.log.track(
                        self.lev, f"Unable to remove {self.botlock_fname} on server.", True
                    )
                self.gen_msg_contents += (
                    f"ssh command returned error code {ssh_cmd.returncode}\n"
                )
            if self.cfg.tracking:
                self.log.track(
                    self.lev,
                    f"Removed {self.botlock_fname} on server.",
                    True,
                )
        except Exception as e:
            if self.cfg.tracking:
                self.log.track(
                    self.lev,
                    f"Unable to create {self.botlock_fname} on server. {ssh_cmd.returncode}.",
                    True,
                )
            return False

        os.chdir(self.original_dir)
        return

    def run_hb_proc(self):
        # set timer when process starts
        # if self.nepi_args.hbto > 0:
        #     thr=Timer(self.nepi_args.hbto, self.hb_early_terminate)
        #     thr.start()
        #     if self.cfg.tracking:
        #         self.log.track(self.lev, "HB815: Received STOP command. Terminating early.", True)
        #         self.log.track(self.lev, f"Current working directory is: {self.original_dir}.", True)
        self.check_hb_dirs()
        self.transfer_files()

        return True

