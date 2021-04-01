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
from nepi_edge_sw_mgr import NepiEdgeSwMgr

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
        self.botlock_fname = '.botlock'  # written on server in home directory while bot is busy on server.
        self.server_do_dir = 'Data'  # where the dirs and files under nepi_home/hb/do go.
        self.server_log_dir = 'NEPI-logs'
        self.server_dt_dir = 'dt'  # not used yet
        self.sw_dir = 'Software'
        # self.bot_do_dir = os.path.abspath('/'.join([botdefs.nepi_home, self.cfg.hb_dir_outgoing]))
        # self.bot_dt_dir = os.path.abspath('/'.join([botdefs.nepi_home, self.cfg.hb_dir_incoming]))
        self.bot_log_dir = os.path.abspath('/'.join([botdefs.nepi_home, "log"]))
        # self.bot_log_dir=self.cfg.log_dir
        self.time_skew = 5  # number of seconds allowable diff in timestamps

        self.sw_mgr = NepiEdgeSwMgr()

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

    # build a command line for the server
    def run_server_cmd(self, cmd_line, msg_success, msg_failed):
        cmd = [
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
            cmd_line,
        ]
        try:
            ssh_cmd = subprocess.run(
                cmd,
                universal_newlines=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            if ssh_cmd.returncode != 0:
                if self.cfg.tracking:
                    self.log.track(
                        self.lev, f"{msg_failed} retcode={ssh_cmd.returncode}", True
                    )
                # self.gen_msg_contents += (
                #     f"ssh command returned error code {ssh_cmd.returncode}\n"
                # )
                return True
            else:
                if self.cfg.tracking:
                    self.log.track(
                        self.lev,
                        f"{msg_success}",
                        True,
                    )
                return False
        except Exception as e:
            if self.cfg.tracking:
                self.log.track(
                    self.lev,
                    f"{msg_failed} (rc={ssh_cmd.returncode})",
                    True,
                )
            return False

    # run a command line on the bot
    def run_local_cmd(self, cmd_line, msg_success, msg_failed):
        # cmd = f"sh -c '{cmd_line}'"
        try:
            ssh_cmd = subprocess.run(
                cmd_line,
                shell=True,
                universal_newlines=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            if ssh_cmd.returncode != 0:
                if self.cfg.tracking:
                    self.log.track(
                        self.lev, f"{msg_failed} retcode={ssh_cmd.returncode}", True
                    )
                # self.gen_msg_contents += (
                #     f"ssh command returned error code {ssh_cmd.returncode}\n"
                # )
                return True
            else:
                if self.cfg.tracking:
                    self.log.track(
                        self.lev,
                        f"{msg_success}",
                        True,
                    )
                return False
        except Exception as e:
            if self.cfg.tracking:
                self.log.track(
                    self.lev,
                    f"{msg_failed} (rc={ssh_cmd.returncode})",
                    True,
                )
            return False

    # execute rsync command on bot from bot to server
    def run_rsync_cmd1(self, work_dir, src_dir, dst_dir, logfile, msg_success, msg_failed):
        rsync_cmd = None
        try:
            os.chdir(work_dir)
            args_rsync = [
                "rsync",
                f"--log-file={logfile}",
                "--remove-source-files",
                # f"--modify-window={self.time_skew}",
                "--stats",
                "-rvatiRLzte",
                f"ssh -p {self.cfg.hb_ip.port} -o StrictHostKeyChecking=no -o GlobalKnownHostsFile=/dev/null -o UserKnownHostsFile=/dev/null -i {self.ssh_key_file}",
                f"{src_dir}",
                f"{self.dev_id_str}@{self.cfg.hb_ip.host}:{dst_dir}",
            ]
            print(f"{' '.join(args_rsync)}")
            rsync_cmd = subprocess.run(
                args_rsync,
                universal_newlines=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            if rsync_cmd.returncode != 0:
                self.gen_msg_contents += f"{msg_failed} [{rsync_cmd.returncode}]"
                if self.cfg.tracking:
                    self.log.track(
                        self.lev,
                        f"{msg_failed} [{rsync_cmd.returncode}]",
                        True,
                    )
                return True
            else:
                if self.cfg.tracking:
                    self.log.track(
                        self.lev,
                        f"{msg_success}",
                        True,
                    )
                return False
        except Exception as e:
            if self.cfg.tracking:
                self.log.track(
                    self.lev,
                    f"{msg_failed} (rc={rsync_cmd.returncode})",
                    True,
                )
        return True

    # execute rsync command on bot from bot to server. Do not remove source files.
    # TODO: consolidate with run_rsync_cmd1 later
    def run_rsync_cmd1a(self, work_dir, src_dir, dst_dir, logfile, msg_success, msg_failed):
        rsync_cmd = None
        try:
            os.chdir(work_dir)
            args_rsync = [
                "rsync",
                f"--log-file={logfile}",
                "--stats",
                "-rvatiRLzte",
                f"ssh -p {self.cfg.hb_ip.port} -o StrictHostKeyChecking=no -o GlobalKnownHostsFile=/dev/null -o UserKnownHostsFile=/dev/null -i {self.ssh_key_file}",
                f"{src_dir}",
                f"{self.dev_id_str}@{self.cfg.hb_ip.host}:{dst_dir}",
            ]
            print(f"{' '.join(args_rsync)}")
            rsync_cmd = subprocess.run(
                args_rsync,
                universal_newlines=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            if rsync_cmd.returncode != 0:
                self.gen_msg_contents += f"{msg_failed} [{rsync_cmd.returncode}]"
                if self.cfg.tracking:
                    self.log.track(
                        self.lev,
                        f"{msg_failed} [{rsync_cmd.returncode}]",
                        True,
                    )
                return True
            else:
                if self.cfg.tracking:
                    self.log.track(
                        self.lev,
                        f"{msg_success}",
                        True,
                    )
                return False
        except Exception as e:
            if self.cfg.tracking:
                self.log.track(
                    self.lev,
                    f"{msg_failed} (rc={rsync_cmd.returncode})",
                    True,
                )
        return True

    # execute rsync command on bot from server to bot
    def run_rsync_cmd2(self, work_dir, src_dir, dst_dir, logfile, msg_success, msg_failed):
        rsync_cmd = None
        try:
            os.chdir(work_dir)
            args_rsync = [
                "rsync",
                f"--log-file={logfile}",
                "--remove-source-files",
                # f"--modify-window={self.time_skew}",
                "--stats",
                "-rvatiRLzte",
                f"ssh -p {self.cfg.hb_ip.port} -o StrictHostKeyChecking=no -o GlobalKnownHostsFile=/dev/null -o UserKnownHostsFile=/dev/null -i {self.ssh_key_file}",
                f"{src_dir}",
                f"{dst_dir}",

            ]
            print(f"{' '.join(args_rsync)}")
            rsync_cmd = subprocess.run(
                args_rsync,
                universal_newlines=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            if rsync_cmd.returncode != 0:
                self.gen_msg_contents += f"{msg_failed} [{rsync_cmd.returncode}]"
                if self.cfg.tracking:
                    self.log.track(
                        self.lev,
                        f"{msg_failed} [{rsync_cmd.returncode}]",
                        True,
                    )
                    return True
                else:
                    if self.cfg.tracking:
                        self.log.track(
                            self.lev,
                            f"{msg_success}",
                            True,
                        )
                    return False
        except Exception as e:
            if self.cfg.tracking:
                self.log.track(
                    self.lev,
                    f"{msg_failed} (rc={rsync_cmd.returncode})",
                    True,
                )
            return True

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
        # self.gen_msg_contents += f"Changed directory to: {self.hb_dir}.\n"
        try:
            # self.gen_msg_contents += f"Created do dt dt/Software directories.\n"
            Path(f"{self.cfg.hb_dir_outgoing}").mkdir(
                mode=0o755, parents=True, exist_ok=True
            )
            Path(f"{self.cfg.hb_dir_incoming}").mkdir(
                mode=0o755, parents=True, exist_ok=True
            )
            Path(f"{self.cfg.hb_dir_incoming}/{self.sw_dir}").mkdir(
                mode=0o755, parents=True, exist_ok=True
            )

            if self.cfg.tracking:
                self.log.track(
                    self.lev,
                    f"Created {self.cfg.hb_dir_outgoing} {self.cfg.hb_dir_incoming} {self.cfg.hb_dir_incoming}/{self.sw_dir} dirs.",
                    True,
                )
        except Exception as e:
            if self.cfg.tracking:
                self.log.track(
                    self.lev,
                    f"Unable to create local {self.cfg.hb_dir_outgoing} {self.cfg.hb_dir_incoming} {self.cfg.hb_dir_incoming}/{self.sw_dir} directories.",
                    True,
                )
        if bothelp.exit_event_hb.is_set():
            if self.cfg.tracking:
                self.log.track(self.lev, "HB811: Received STOP command. Terminating early.", True)
                self.log.track(
                    self.lev, f"Current working directory is: {self.original_dir}.", True
                )
            return True

        # write a lock file on server so server knows bot is screwing off.
        self.run_server_cmd(f'echo "BOTLOCK" > .botlog', 'Wrote bot lock file on server.',
                            'Unable to write bot lock file on server.')

        # make required directories on server if they do not exist.
        self.run_server_cmd(f'mkdir -p {self.server_log_dir} {self.server_do_dir} {self.sw_dir}',
                            f'Created {self.server_log_dir} {self.server_do_dir} {self.sw_dir} dirs on server.',
                            f'Could not create {self.server_log_dir} {self.server_do_dir} {self.sw_dir} dirs on server.')

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
        # os.chdir(f"{self.hb_dir}")
        # if self.cfg.tracking:
        #     self.log.track(self.lev, f"Changed directory to: {self.hb_dir}.", True)

        # transfer do files from bot to server
        self.run_rsync_cmd1(f"{self.hb_dir}", "do/", self.server_do_dir, f"{self.bot_log_dir}/bot_do_transfer.log",
                            'Bot DO file transfer successful.', 'Bot DO file transfer failed.')

        # move do directory up 1 level on server and cleanup
        self.run_server_cmd(f"cd {self.server_do_dir}; cp -r do/* .; rm -fr do",
                            'Bot moved DO dir to proper place on server',
                            'Bot could not move DO data to proper place on server')

        # remove empty directories from bot DO
        # self.run_local_cmd(f"cd {self.hb_dir}/do", f"Removed empty directories in {self.hb_dir}/do",
        #                    f"Unable to remove empty directories in {self.hb_dir}/do")
        self.run_local_cmd(f"cd {self.hb_dir}/do; find -L . -type d -empty -delete",
                           f"Removed empty directories in {self.hb_dir}/do",
                           f"Unable to remove empty directories in {self.hb_dir}/do")

        # transfer software files from server to bot
        self.run_rsync_cmd2(f"{self.hb_dir}/dt",
                            f"{self.dev_id_str}@{self.cfg.hb_ip.host}:{self.sw_dir}/",
                            ".",
                            f"{self.bot_log_dir}/bot_sw_transfer.log",
                            'Bot SW transfer succeeded.', 'Bot SW transfer failed.')

        # remove empty directories in server Software directories
        self.run_server_cmd(f"cd Software; find . -type d -empty -delete",
                            'Successfully cleaned up Software dir on server',
                            'Could not clean up Software dir on server.')

        # remove a lock file on server so server knows bot is finished screwing off.
        self.run_server_cmd(f'rm -f .botlog', 'Deleted bot lock file on server.',
                            'Unable to delete bot lock file on server.')

    def transfer_logs(self):
        # transfer logs from bot to server for current run
        self.run_rsync_cmd1a(f"{self.hb_dir}/..", f"log/", self.server_log_dir,
                             f"{self.bot_log_dir}/bot_log_transfer.log",
                             'Bot LOG file transfer successful.', 'Bot LOG file transfer failed.')

        # move log directory up 1 level on server and cleanup
        self.run_server_cmd(f"cd {self.server_log_dir}; mv log/* .; rm -fr log",
                            'Bot moved log dir to proper place on server',
                            'Bot could not move log data to proper place on server')


    def run_sw_mgr(self):
        os.chdir(f"{self.hb_dir}/dt")
        # Check whether any "Software" was transferred from server
        local_sw_dir = f"./{self.sw_dir}"
        if (os.path.isdir(local_sw_dir)) is False or (len(os.listdir(f"./{self.sw_dir}")) == 0):
            if self.cfg.tracking:
                self.log.track(
                    self.lev,
                    "Empty software folder: Will not start the software manager",
                    True,
                )
        else:
            if self.cfg.tracking:
                self.log.track(
                    self.lev,
                    "Non-empty Software folder: Starting the software manager",
                    True,
                )
            try:
                self.sw_mgr.process_sw_folder(f"{self.sw_dir}", results_path='../../log')
            except Exception as e:
                self.log.track(
                    self.lev + 2,
                    f"Software manager encountered an error: {e}",
                    True,
                )

            # Clear the local software folder
            self.run_local_cmd(f"rm -rf ./{self.sw_dir}/*",
                               f"Removed empty directories in {self.hb_dir}/do",
                               f"Unable to remove empty directories in {self.hb_dir}/do")



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
        self.run_sw_mgr()

        return True
