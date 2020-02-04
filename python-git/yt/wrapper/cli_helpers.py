from __future__ import print_function

import yt.logger as logger
import yt.yson as yson
from yt.common import get_value, update
from yt.wrapper.errors import YtOperationFailedError, YtError
from yt.wrapper.operation_commands import format_operation_stderrs
from yt.wrapper.common import get_binary_std_stream

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
    except:
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

def get_role_members(node):
    members = []
    for n in node:
        members.append(n.values()[0])
    return members

def print_aligned(left, right):
    print("{:<30}{}".format(left, right))

def pretty_print_acls(acl_data, responsibles_data, object_type, show_inherited=False):
    if object_type in ["path", "account"]:
        acl = []
        for ace in acl_data.get("roles", []):
            if ace.get("state") != "granted":
                logger.warn("Found role not in granted state [%s]" % ace)
                continue
            if ace.get("inherited") == True and not show_inherited:
                continue
            permissions = ace.get("permissions")
            if "user" in ace.get("subject").keys():
                subjects = ace.get("subject").values()[0]
            elif "group" in ace.get("subject").keys():
                subjects = "%s (idm-group:%s)" % (
                        ace.get("subject").get("url"),
                        ace.get("subject").get("group")
                )

            acl.append({"permissions": permissions, "subjects": subjects})

        resps = get_role_members(responsibles_data["responsible"].get("responsible", []))
        read_appr = get_role_members(responsibles_data["responsible"].get("read_approvers", []))
        auditors = get_role_members(responsibles_data["responsible"].get("auditors", []))

        disable_inheritance_resps = responsibles_data["responsible"].get("disable_inheritance", False)
        boss_approval = responsibles_data["responsible"].get("require_boss_approval", False)
        inherit_acl = responsibles_data.get("inherit_acl", False)

        print_aligned("Responsibles:", " ".join(resps))
        print_aligned("Read approvers:", " ".join(read_appr))
        print_aligned("Auditors:", " ".join(auditors))
        print_aligned("Inherit responsibles:", (not disable_inheritance_resps))
        print_aligned("Boss approval required:", boss_approval)
        print_aligned("Inherit ACL:", inherit_acl)

        print("ACL roles:")
        for ace in acl:
            print("  {:<20} - {}".format(ace["subjects"], "/".join(ace["permissions"])))

    elif object_type == "group":
        resps = get_role_members(acl_data["resps"]["responsibles"]["responsible"])

        print_aligned("Responsibles:", " ".join(resps))
        print("Members:")
        for member in acl_data["roles"]["members"]:
            m = ""
            if "user" in member.keys():
                m = member["user"]
            elif "group" in member.keys():
                m = "{} (idm-group:{})".format(
                    member.get("url"),
                    member.get("group")
                )
            print("  {}".format(m))
    else:
        raise NotImplementedError("showing acls of '{}' objects is not implemented".format(object_type))
