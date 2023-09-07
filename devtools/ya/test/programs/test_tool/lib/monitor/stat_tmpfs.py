try:
    from .statvfs import statvfs
except ImportError:
    statvfs = None

from devtools.ya.test.programs.test_tool.lib.monitor import pollmon


def is_stat_tmpfs_supported():
    return bool(statvfs)


class TmpfsUsageMonitor(pollmon.PollMonitor):
    def __init__(self, mount_point, delay):
        super(TmpfsUsageMonitor, self).__init__(delay)
        self.mount_point = mount_point
        self.inodes_used = 0
        self.mem_used = 0
        self.block_size = 0

    def setup_monitor(self):
        st = self.get_vfs_stats()
        assert st['f_fsid'] == 0, "Mount point doesn't look like tmpfs: %s f_fsid=%d" % (self.mount_point, st['f_fsid'])
        self.block_size = st['f_bsize']

    def poll(self):
        st = self.get_vfs_stats()

        iused = st['f_files'] - st['f_ffree']
        if iused > self.inodes_used:
            self.inodes_used = iused

        mused = self.block_size * (st['f_blocks'] - st['f_bfree'])
        if mused > self.mem_used:
            self.mem_used = mused

    def get_vfs_stats(self):
        return statvfs(self.mount_point)

    def get_max_mem_used(self):
        # returns kilobytes
        return self.mem_used // 1024

    def get_max_inodes_used(self):
        return self.inodes_used
