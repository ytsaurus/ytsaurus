#pragma once

#include <sys/statvfs.h>

using ulong = unsigned long;

struct TStatVfs {
    ulong f_bsize;
    ulong f_frsize;
    ulong f_blocks;
    ulong f_bfree;
    ulong f_bavail;
    ulong f_files;
    ulong f_ffree;
    ulong f_favail;
    ulong f_fsid;
    ulong f_flag;
    ulong f_namemax;
};

int get_statvfs(const char* path, TStatVfs& st) {
    struct statvfs buf;
    int ret = statvfs(path, &buf);
    if (ret)
        return ret;

    st = {
        buf.f_bsize,
        buf.f_frsize,
        buf.f_blocks,
        buf.f_bfree,
        buf.f_bavail,
        buf.f_files,
        buf.f_ffree,
        buf.f_favail,
        buf.f_fsid,
        buf.f_flag,
        buf.f_namemax,
    };
    return 0;
}
