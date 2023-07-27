# coding=utf-8

from __future__ import print_function
import os

import exts.process
import exts.windows


def get_handles_count():
    return exts.windows.get_process_handle_count(exts.windows.get_current_process())


def get_file_descriptiors_count():
    import resource

    limit = resource.getrlimit(resource.RLIMIT_NOFILE)[0]
    count = 0
    for fd in range(limit):
        try:
            os.fstat(fd)
        except OSError:
            pass
        else:
            count += 1
    return count


def main():
    if exts.windows.on_win():
        print(get_handles_count())
    else:
        print(get_file_descriptiors_count())


if __name__ == '__main__':
    main()
