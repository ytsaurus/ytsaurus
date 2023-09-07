import contextlib
import logging
import os
import shutil
import subprocess
import sys
import traceback

from yalibrary import tools

logger = logging.getLogger(__name__)


@contextlib.contextmanager
def arc_repo():
    token_path = "repo/.arc/token"
    mount_done = False
    home = os.environ.get('HOME')
    try:
        if not os.path.exists(os.path.abspath("repo")):
            os.mkdir("repo")
        arcadia = os.path.abspath("repo/arcadia")
        if not os.path.exists(arcadia):
            os.mkdir(arcadia)
            os.mkdir("repo/store")

            os.mkdir("repo/.arc")
            os.chmod("repo/.arc", 0o700)

            if os.environ.get("TEST_ARC_TOKEN"):
                shutil.copyfile(os.environ.get("TEST_ARC_TOKEN"), token_path)
                os.chmod(token_path, 0o400)
            else:
                shutil.copyfile(os.path.expanduser("~/.arc/token"), token_path)

            os.environ['HOME'] = os.path.join(os.getcwd(), 'repo')

        call_arc(['mount', '-m', arcadia, '-S', 'store'], cwd='repo')
        mount_done = True
        yield arcadia
    except Exception:
        traceback.print_exc(file=sys.stderr)
        raise
    finally:
        os.environ['HOME'] = home
        if mount_done:
            call_arc(['reset', 'HEAD'], cwd='repo/arcadia')
            call_arc(['clean', '-d'], cwd='repo/arcadia')
            call_arc(['unmount', 'arcadia/'], cwd='repo')
        if os.path.exists(token_path):
            os.unlink(token_path)


def call_arc(args, cwd):
    cmd = [tools.tool('arc')] + args
    logger.debug("Call '%s' in '%s'", cmd, cwd)
    return subprocess.check_call(cmd, cwd=cwd)
