import os
import logging


def test_run_in_qemu_works():
    for e in sorted(os.listdir("/")):
        logging.info("qemu_vm: ls /: {}".format(e))
    # /run_test.sh is a special runner that is brought in qemu
    assert os.path.exists("/run_test.sh")
