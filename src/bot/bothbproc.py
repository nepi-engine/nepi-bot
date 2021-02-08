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

import botdefs
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
        if self.cfg.tracking:
            self.log.track(self.lev, "Created HbProc Class Object.", True)

    # double check hb directory structure on nepibot and server
    def check_hb_dirs(self):
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
            Path(f"logs").mkdir(mode=0o755, parents=True, exist_ok=True)
            self.gen_msg_contents += f"Created {self.cfg.hb_dir_outgoing} {self.cfg.hb_dir_incoming} logs directories.\n"
            if self.cfg.tracking:
                self.log.track(
                    self.lev,
                    f"Created {self.cfg.hb_dir_outgoing} {self.cfg.hb_dir_incoming} logs dirs.",
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

        # check directories on server
        args_sshcmd = [
            "ssh",
            "-p",
            f"{self.cfg.hb_ip.port}",
            "-i",
            f"{self.ssh_key_file}",
            f"{self.dev_id_str}@{self.cfg.hb_ip.host}",
            f"mkdir -p {self.cfg.hb_dir_outgoing} {self.cfg.hb_dir_incoming} hb/logs",
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
                    f"Created {self.cfg.hb_dir_outgoing} {self.cfg.hb_dir_incoming} hb/logs dirs on server.",
                    True,
                )
        except Exception as e:
            if self.cfg.tracking:
                self.log.track(
                    self.lev,
                    f"Unable to create {self.cfg.hb_dir_outgoing} {self.cfg.hb_dir_incoming} hb/logs dirs on server {ssh_cmd.returncode}.",
                    True,
                )
            return False
        return True

    # transfer files from nepibot to server
    def transfer_files(self):
        datetimestr = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        os.chdir(f"{self.hb_dir}")
        if self.cfg.tracking:
            self.log.track(self.lev, f"Changed directory to: {self.hb_dir}.", True)

        # transfer do files from bot to server

        args_rsync = [
            "rsync",
            f"--log-file=logs/{datetimestr}do.log",
            "--remove-source-files",
            "--stats",
            "-rIvvvatiRzte",
            f"ssh -p {self.cfg.hb_ip.port} -i {self.ssh_key_file}",
            f"do",
            f"{self.dev_id_str}@{self.cfg.hb_ip.host}:~/{self.cfg.hb_dir_outgoing}",
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
            f"--include={datetimestr}do.log",
            "-rvvvatiRzte",
            f"ssh -p {self.cfg.hb_ip.port} -i {self.ssh_key_file}",
            f"logs",
            f"{self.dev_id_str}@{self.cfg.hb_ip.host}:~/{self.cfg.hb_dir_outgoing}",
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

        # transfer dt files from server to bot

        args_rsync = [
            "rsync",
            f"--log-file=logs/{datetimestr}dt.log",
            "--remove-source-files",
            "--stats",
            "-rIvvvatiRzte",
            f"ssh -p {self.cfg.hb_ip.port} -i {self.ssh_key_file}",
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

        # transfer dt log file from bot to server

        args_rsync = [
            "rsync",
            f"--include={datetimestr}dt.log",
            "-rvvvatiRzte",
            f"ssh -p {self.cfg.hb_ip.port} -i {self.ssh_key_file}",
            f"logs",
            f"{self.dev_id_str}@{self.cfg.hb_ip.host}:~/hb/clone",
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

        # mv dt subdirectory rsync creates into dt.
        completed = subprocess.run(
            ["mv -f dt/hb/clone/dt/* dt"],
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

        os.chdir(self.original_dir)
