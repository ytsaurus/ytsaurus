from yt.wrapper.operation_commands import get_stderrs
import yt.wrapper as yt

import sys

def main():
    yt.config["operation_tracker"]["stderr_download_thread_count"] = 2
    get_stderrs(sys.argv[1], False)

if __name__ == "__main__":
    main()
