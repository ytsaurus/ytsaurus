import yt.logger as logger
from yt.wrapper.errors import YtOperationFailedError, YtError

import os
import sys
import traceback

def writelines_silently(lines):
    try:
        for line in lines:
            sys.stdout.write(line)
    except IOError as err:
        # Trying to detect case of broken pipe
        if err.errno == 32:
            sys.exit(1)
        raise
    except Exception:
        raise
    except:
        # Case of keyboard abort
        try:
            sys.stdout.flush()
        except IOError:
            sys.exit(1)
        raise

def die(message=None, return_code=1):
    if message is not None:
        print >>sys.stderr, message
    if "YT_LOG_EXIT_CODE" in os.environ:
        logger.error("Exiting with code %d", return_code)
    sys.exit(return_code)

def run_main(main_func):
    try:
        main_func()
    except KeyboardInterrupt:
        die("Shutdown requested... exiting")
    except YtOperationFailedError as error:
        print >>sys.stderr, str(error)
        if "stderrs" in error.attributes:
            print >>sys.stderr
            print >>sys.stderr, "Stderrs of failed jobs:"
            print >>sys.stderr, error.attributes["stderrs"]
        die()
    except YtError as error:
        die(str(error), error.code)
    except Exception:
        traceback.print_exc(file=sys.stderr)
        die()



