#!/usr/bin/python2.7

import re
import pprint
import yt.wrapper as yt
import argparse
import subprocess

def parse_clickhouse_error_codes():
    with open("../../contrib/libs/clickhouse/dbms/src/Common/ErrorCodes.cpp", "r") as fd:
        result = dict()
        for line in fd: 
            match = re.match(".*extern const int ([^ ]*) = ([0-9]*);", line)
            if match and match.groups():
                name, key = match.groups()
                result[key] = name
    return result

def parse_yt_error_codes():
    files = subprocess.check_output(["fgrep", "-r", "-l", "DEFINE_ENUM(EErrorCode", "../../yt"])
    files = files.strip().split()
    result = dict()
    for f in files:
        with open(f, "r") as fd:
            current_namespace = ""
            inside_enum = False
            was = False
            for line in fd:
                start_namespace_match = re.match("namespace ([^ ]*).*", line.strip())
                end_namespace_match = re.match(".*// namespace ([^ ]*).*", line.strip())
                start_enum_match = re.match(".*DEFINE_ENUM\(EErrorCode.*", line.strip())
                error_code_match = re.match(".*\(\(([^ ]*)\).*\(([0-9]*)\)\).*", line.strip())
                end_enum_match=re.match("[^(]*\)[^(]*", line.strip())
                if start_namespace_match and start_namespace_match.groups():
                    namespace = start_namespace_match.groups()[0]
                    if current_namespace != "":
                        current_namespace += "::"
                    current_namespace += namespace
                elif end_namespace_match and end_namespace_match.groups():
                    namespace = end_namespace_match.groups()[0]
                    assert current_namespace.endswith(namespace)
                    current_namespace = current_namespace[:-len(namespace)]
                    if current_namespace.endswith("::"):
                        current_namespace = current_namespace[:-2]
                elif start_enum_match:
                    inside_enum = True
                    was = True
                elif inside_enum and error_code_match and error_code_match.groups():
                    name, key = error_code_match.groups()
                    assert key not in result
                    result[key] = current_namespace + "::" + name
                elif end_enum_match:
                    inside_enum = False
            assert was
    return result

def main():
    parser = argparse.ArgumentParser(description="Gather error codes of CH and YT")
    parser.add_argument("--dry-run", action="store_true", help="Just print gathered information")
    args = parser.parse_args()
    clickhouse_error_codes = parse_clickhouse_error_codes()
    yt_error_codes = parse_yt_error_codes()

    if not args.dry_run:
        if not yt.exists("//sys/clickhouse/error_codes"):
            yt.create("document", "//sys/clickhouse/error_codes", attributes={"value": {}})
        yt.set("//sys/clickhouse/error_codes/clickhouse", clickhouse_error_codes)
        yt.set("//sys/clickhouse/error_codes/yt", yt_error_codes)
    else:
        pprint.pprint(clickhouse_error_codes)
        pprint.pprint(yt_error_codes)

if __name__ == "__main__":
    main()
