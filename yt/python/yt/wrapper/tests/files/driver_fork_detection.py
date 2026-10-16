from yt_driver_bindings import Driver

import os
import subprocess
import sys

# Dummy URL: driver creation does not connect and get_command_descriptors is local,
# so no cluster is needed.
DRIVER_CONFIG = {
    "connection_type": "rpc",
    "cluster_url": "http://localhost:1",
    "api_version": 4,
    "enable_fork_detection": True,
}


def main():
    driver = Driver(DRIVER_CONFIG, connection_type="rpc")
    driver.get_command_descriptors()

    if sys.argv[1:] == ["fresh-child"]:
        return

    pid = os.fork()
    if pid == 0:
        # Forked child: any driver usage must raise.
        try:
            driver.get_command_descriptors()
            os._exit(1)
        except RuntimeError as error:
            os._exit(0 if "forked" in str(error) else 2)

    assert os.waitpid(pid, 0)[1] == 0

    # The parent is not affected by the fork.
    driver.get_command_descriptors()

    # A freshly started process (fork+exec) is not affected either.
    subprocess.check_call([sys.executable, sys.argv[0], "fresh-child"])


if __name__ == "__main__":
    main()
