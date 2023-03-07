#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function

import datetime
import os
import sys
import time
import tempfile

class TimeoutError(Exception):
    pass

class JobEvents(object):
    """ EventsOnFs helps to exchange information between test code
    and test MR jobs about different events."""

    BREAKPOINT_ALL_RELEASED = "all_released"

    def __init__(self, tmpdir):
        self._tmpdir = tmpdir
        self._breakpoints = set()

    def notify_event(self, event_name):
        file_name = self._get_event_filename(event_name)
        with open(file_name, "w"):
            pass

    def notify_event_cmd(self, event_name):
        return "touch \"{0}\"".format(self._get_event_filename(event_name))

    def wait_event(self, event_name, timeout=datetime.timedelta(seconds=60)):
        file_name = self._get_event_filename(event_name)
        deadline = datetime.datetime.now() + timeout
        while True:
            if os.path.exists(file_name):
                break
            if datetime.datetime.now() > deadline:
                raise TimeoutError("Timeout exceeded while waiting for {0}".format(event_name))
            time.sleep(0.1)

    def check_event(self, event_name):
        file_name = self._get_event_filename(event_name)
        return os.path.exists(file_name)

    def wait_event_cmd(self, event_name, timeout=datetime.timedelta(seconds=60)):
        return (
            " {{ wait_limit={wait_limit}\n"
            " while ! [ -f {event_file_name} ]\n"
            " do\n"
            "     sleep 0.1 ; ((wait_limit--)) ;\n"
            "     if [ $wait_limit -le 0 ] ; then \n"
            "         echo timeout for event {event_name} exceeded >&2 ; exit 1 ;\n"
            "     fi\n"
            " done \n"
            "}}").format(
                event_name=event_name,
                event_file_name=self._get_event_filename(event_name),
                wait_limit=timeout.seconds*10)

    def breakpoint_cmd(self, breakpoint_name="default", timeout=datetime.timedelta(seconds=60)):
        """ Returns shell command that inserts breakpoint into job.
            Once job reaches breakpoint it pauses its execution and waits until this breakpoint
            is released for this job or for all jobs."""
        job_breakpoint = self._get_breakpoint_filename(breakpoint_name, "$YT_JOB_ID")
        breakpoint_released = self._get_breakpoint_filename(breakpoint_name, self.BREAKPOINT_ALL_RELEASED)
        wait_limit = timeout.seconds * 10
        self._create_breakpoint(breakpoint_name)

        return (
            """ {{ wait_limit={wait_limit}\n """
            """ touch {job_breakpoint}\n """
            """ while [ -f {job_breakpoint} -a ! -f {breakpoint_released} ] ; do \n """
            """   sleep 0.1 ; ((wait_limit--)) ;\n """
            """   if [ $wait_limit -le 0 ] ; then \n """
            """       echo timeout for breakpoint {breakpoint_name} exceeded >&2 ; exit 1 ;\n """
            """   fi\n """
            """ done\n """
            """ }} """
        ).format(
            breakpoint_name=breakpoint_name,
            job_breakpoint=job_breakpoint,
            breakpoint_released=breakpoint_released,
            wait_limit=wait_limit)

    def execute_once(self, cmd):
        shared_file = self._create_temp_file()
        tmp_file_pattern = os.path.join(self._tmpdir, "XXXXXX")
        # We use race-free solution for mv -n (https://unix.stackexchange.com/a/248758)
        return 'tmpfile=$(mktemp "{tmp_file_pattern}"); '\
               'touch "$tmpfile"; '\
               'ln -PT "$tmpfile" "{shared_file}" && rm "$tmpfile"; '\
               'if [ ! -e "$tmpfile" ]; then {cmd}; fi; '\
                .format(
                    tmp_file_pattern=tmp_file_pattern,
                    shared_file=shared_file,
                    cmd=cmd)

    def wait_breakpoint(self, breakpoint_name="default", job_id=None, job_count=None, check_fn=None, timeout=datetime.timedelta(seconds=60)):
        """ Wait until some job reaches breakpoint.
            Return list of all jobs that are currently waiting on this breakpoint """
        self._verify_breakpoint_created(breakpoint_name)

        if job_id is not None and check_fn is None:
            check_fn = lambda job_id_list: job_id in job_id_list

        if job_count is not None and check_fn is None:
            check_fn = lambda job_id_list: len(job_id_list) >= job_count

        deadline = datetime.datetime.now() + timeout
        breakpoint_prefix = "breakpoint_" + breakpoint_name + "_"
        while True:
            file_name_list = os.listdir(self._tmpdir)
            job_id_list = []
            for file_name in file_name_list:
                if not file_name.startswith(breakpoint_prefix):
                    continue
                cur_job_id = file_name[len(breakpoint_prefix):]
                if cur_job_id == self.BREAKPOINT_ALL_RELEASED:
                    raise RuntimeError("Breakpoint {0} was released for all jobs".format(breakpoint_name))
                job_id_list.append(cur_job_id)

            if check_fn is None:
                if job_id_list:
                    return job_id_list
            else:
                if check_fn(job_id_list):
                    return job_id_list

            if datetime.datetime.now() > deadline:
                raise TimeoutError(
                    "Timeout exceeded while waiting for breakpoint {0}, current jobs {1}"
                    .format(breakpoint_name, job_id_list))

            time.sleep(0.1)

    def release_breakpoint(self, breakpoint_name="default", job_id=None):
        """ Releases breakpoint so given job or all jobs can continue execution.

            job_id: id of a job that should continue execution,
                    if job_id is None then all jobs continue execution and all future jobs
                    will skip this breakpoint. """
        self._verify_breakpoint_created(breakpoint_name)

        print("Releasing breakpoint {0}, job id {1}".format(breakpoint_name, job_id), file=sys.stderr)

        if job_id is None:
            with open(self._get_breakpoint_filename(breakpoint_name, self.BREAKPOINT_ALL_RELEASED), "w"):
                pass
        else:
            file_name = self._get_breakpoint_filename(breakpoint_name, job_id)
            if not os.path.exists(file_name):
                raise RuntimeError("Job: {0} is not waiting on breakpoint {1}".format(job_id, breakpoint_name))
            os.remove(file_name)

    def with_breakpoint(self, cmd):
        if "BREAKPOINT" not in cmd:
            raise ValueError("Command doesn't have BREAKPOINT: {0}".format(cmd))
        result = cmd.replace("BREAKPOINT", self.breakpoint_cmd(), 1)
        if "BREAKPOINT" in result:
            raise ValueError("Command has multiple BREAKPOINT: {0}".format(cmd))
        return result

    def _create_breakpoint(self, breakpoint_name):
        self._breakpoints.add(breakpoint_name)

    def _verify_breakpoint_created(self, breakpoint_name):
        if not breakpoint_name in self._breakpoints:
            raise ValueError("Breakpoint `{0}' was never created (breakpoint is created when breakpoint_cmd is called)".format(breakpoint_name))

    def _get_breakpoint_filename(self, breakpoint_name, job_id):
        if not breakpoint_name:
            raise ValueError("breakpoint_name must be non empty")
        return os.path.join(self._tmpdir, "breakpoint_{name}_{job_id}".format(
            name=breakpoint_name,
            job_id=job_id))

    def _get_event_filename(self, event_name):
        if not event_name:
            raise ValueError("event_name must be non empty")
        return os.path.join(self._tmpdir, event_name)

    def _create_temp_file(self):
        fd, path = tempfile.mkstemp(dir=self._tmpdir)
        os.close(fd)
        os.remove(path)
        return path
