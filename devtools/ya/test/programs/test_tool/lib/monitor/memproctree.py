import collections
import math
import os

import psutil

from devtools.ya.test.programs.test_tool.lib.monitor import pollmon
from yalibrary import formatter


class ProcInfo(object):
    def __init__(self, pid, command, rss, ref, private_dirty, proc):
        self.pid = pid
        self.command = command
        self.rss = rss
        self.ref = ref
        self.private_dirty = private_dirty
        self.proc = proc


class MemProcessTreeMonitor(pollmon.PollMonitor):
    def __init__(self, target_pid, precise_limit=0, delay=1, caption="", cmdline_limit=200):
        super(MemProcessTreeMonitor, self).__init__(delay)
        self.process = psutil.Process(target_pid)
        self.precise_limit = precise_limit
        self.maxrss = 0
        self.max_used = 0
        self.caption = caption
        assert cmdline_limit > 0, cmdline_limit
        self.cmdline_limit = cmdline_limit
        self.precise_tree = False
        self.proc_tree = {}
        self.refine_available = hasattr(psutil.Process(target_pid), 'memory_maps')

    def setup_monitor(self):
        self.maxrss = 0
        self.proc_tree = {}

    def _create_proc_info(self, proc):
        cmd_line = " ".join([os.path.basename(proc.exe())] + proc.cmdline()[1:])
        cmd_line = cmd_line[: self.cmdline_limit].strip()
        return ProcInfo(
            pid=proc.pid,
            command=cmd_line,
            rss=proc.memory_info().rss,
            ref=0,
            private_dirty=0,
            proc=proc,
        )

    def _refine_process_tree(self, tree):
        for procs in tree.values():
            for pi in procs:
                try:
                    mem_map = pi.proc.memory_maps(grouped=True)
                    ref, private_dirty = 0, 0
                    for e in mem_map:
                        ref += e.referenced
                        private_dirty += e.private_dirty
                    pi.ref = ref
                    pi.private_dirty = private_dirty
                except (OSError, psutil.Error):
                    pass

    def poll(self):
        tree = self._get_process_tree()
        rss = sum(pi.rss for procs in tree.values() for pi in procs)
        if rss > self.maxrss:
            self.maxrss = rss
            self.proc_tree = tree
        # Reading process's memory mappings (smaps) is quite expensive.
        # Refine proc info only in case when we need to find out actual mem usage to avoid false-positive blame.
        if self.refine_available and rss > self.precise_limit:
            self._refine_process_tree(tree)
            # fork() is implemented using copy-on-write pages and new-born process's rss doesn't
            # present actual process memory consumption.
            # Take a look at referenced (actually used) or private_dirty (if parent is dead) pages.
            used = sum(
                pi.ref if pi.ref > pi.private_dirty else pi.private_dirty for procs in tree.values() for pi in procs
            )
            if used > self.max_used or not self.precise_tree:
                self.max_used = used
                self.proc_tree = tree
                self.precise_tree = True
        return self

    def _get_process_tree(self):
        tree = collections.defaultdict(list)
        try:
            tree[self.process.ppid] = [self._create_proc_info(self.process)]
            for proc in self.process.children(recursive=True):
                try:
                    tree[proc.ppid()].append(self._create_proc_info(proc))
                except (OSError, psutil.Error):
                    pass
        except (OSError, psutil.Error):
            pass
        return tree

    def get_max_mem_used(self):
        # returns kilobytes
        if self.precise_tree:
            return self.max_used // 1024
        else:
            return self.maxrss // 1024

    def dumps_process_tree(self):
        if not self.proc_tree:
            return "No process tree available"

        max_pid_len = max(int(math.ceil(math.log10(pi.pid))) for procs in self.proc_tree.values() for pi in procs)

        if self.precise_tree:
            line_pattern = "{:>%d} {:>5} {:>5} {:>5} " % max_pid_len
            lines = [line_pattern.format("pid", "rss", "ref", "pdirt") + self.caption]
        else:
            line_pattern = "{:>%d} {:>5} " % max_pid_len
            lines = [line_pattern.format("pid", "rss") + self.caption]

        def fmt_rss(x):
            return "{:>4}".format(formatter.format_size(x))

        def fmt(pi, first_proc, prefix, last_entry):
            pid = pi.pid
            if self.precise_tree:
                proc_info_prefix = prefix.format(pid, fmt_rss(pi.rss), fmt_rss(pi.ref), fmt_rss(pi.private_dirty))
            else:
                proc_info_prefix = prefix.format(pid, fmt_rss(pi.rss))

            proc_info_prefix = proc_info_prefix + ("" if first_proc else "└─ " if last_entry else "├─ ")
            lines.append(proc_info_prefix + pi.command[: self.cmdline_limit - len(proc_info_prefix)])
            if self.proc_tree[pid]:
                dump(pid, False, prefix + ("   " if last_entry else "│  "))

        def dump(pid, first_proc, prefix):
            if not self.proc_tree.get(pid):
                return

            for child in self.proc_tree[pid][:-1]:
                fmt(child, first_proc, prefix, False)

            child = self.proc_tree[pid][-1]
            fmt(child, first_proc, prefix, True)

        dump(self.process.ppid, True, line_pattern)
        return '\n'.join(lines)
