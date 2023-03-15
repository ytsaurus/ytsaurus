from __future__ import print_function

import yt.logger as logger
import yt.yson as yson
from yt.common import get_value, update
from yt.wrapper.errors import YtOperationFailedError, YtError
from yt.wrapper.operation_commands import format_operation_stderrs
from yt.wrapper.common import get_binary_std_stream

try:
    from yt.packages.six import binary_type
except ImportError:
    from six import binary_type

import os
import sys
import traceback
from argparse import Action


def write_silently(strings, force_use_text_stdout=False):
    output_stream = sys.stdout
    if not force_use_text_stdout:
        output_stream = get_binary_std_stream(sys.stdout)

    try:
        for str in strings:
            output_stream.write(str)
    except IOError as err:
        # Trying to detect case of broken pipe
        if err.errno == 32:
            sys.exit(0)
        raise
    except Exception:
        raise
    except:  # noqa
        # Case of keyboard abort
        try:
            sys.stdout.flush()
        except IOError:
            sys.exit(1)
        raise
    finally:
        # To avoid strange trash in stderr in case of broken pipe.
        # For more details look at http://bugs.python.org/issue11380
        try:
            sys.stdout.flush()
        finally:
            try:
                sys.stderr.flush()
            finally:
                pass


def print_to_output(string_or_bytes, output_stream=sys.stdout, eoln=True):
    if isinstance(string_or_bytes, binary_type):
        get_binary_std_stream(output_stream).write(string_or_bytes)
    else:
        output_stream.write(string_or_bytes)

    if eoln:
        output_stream.write("\n")


def die(message=None, return_code=1):
    if message is not None:
        print(message, file=sys.stderr)
    if "YT_LOG_EXIT_CODE" in os.environ:
        logger.error("Exiting with code %d", return_code)
    sys.exit(return_code)


def run_main(main_func):
    try:
        main_func()
    except KeyboardInterrupt:
        die("Shutdown requested... exiting")
    except YtOperationFailedError as error:
        stderrs = None
        if "stderrs" in error.attributes and error.attributes["stderrs"]:
            stderrs = error.attributes["stderrs"]
            del error.attributes["stderrs"]

        print(str(error), file=sys.stderr)
        if stderrs is not None:
            print("\nFailed jobs:", file=sys.stderr)
            print(format_operation_stderrs(stderrs), file=sys.stderr)
        die()
    except YtError as error:
        if "YT_PRINT_BACKTRACE" in os.environ:
            traceback.print_exc(file=sys.stderr)
        die(str(error), error.code)
    except Exception:
        traceback.print_exc(file=sys.stderr)
        die()


class ParseStructuredArgument(Action):
    def __init__(self, option_strings, dest, action_load_method=yson._loads_from_native_str, **kwargs):
        Action.__init__(self, option_strings, dest, **kwargs)
        self.action_load_method = action_load_method

    def __call__(self, parser, namespace, values, option_string=None):
        # Multiple times specified arguments are merged into single dict.
        old_value = get_value(getattr(namespace, self.dest), {})
        new_value = update(old_value, self.action_load_method(values))
        setattr(namespace, self.dest, new_value)


def populate_argument_help(parser):
    old_add_argument = parser.add_argument

    def add_argument(*args, **kwargs):
        help = []
        if kwargs.get("required", False):
            help.append("(Required) ")
        help.append(kwargs.get("help", ""))
        if kwargs.get("action") == "append":
            help.append(" Accepted multiple times.")
        kwargs["help"] = "".join(help)
        return old_add_argument(*args, **kwargs)
    parser.add_argument = add_argument
    parser.set_defaults(last_parser=parser)
    return parser
