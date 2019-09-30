from yt.wrapper.operation_commands import get_jobs_with_error_or_stderr
import yt.wrapper as yt

import signal
import sys

def main():
    signal.signal(signal.SIGINT, signal.default_int_handler)
    yt.config["operation_tracker"]["stderr_download_thread_count"] = 2
    get_jobs_with_error_or_stderr(sys.argv[1], False)

if __name__ == "__main__":
    main()
