import argparse
import hashlib
import http.server
import os
import pathlib
import socket
import subprocess
import sys
import tempfile
import urllib.request

OUT_FILE = pathlib.Path("c++_doc.tar.gz").resolve()

DOXYGEN_CONFIG_TEMPLATE = """
DOXYFILE_ENCODING = UTF-8
PROJECT_NAME      = "C++ YT Wrapper"
PROJECT_BRIEF     = "C++ library for working with YT clusters"
OUTPUT_DIRECTORY  = {output_directory}
RECURSIVE         = NO
INPUT             = mapreduce/yt/interface
EXCLUDE           = mapreduce/yt/interface/logging
FILE_PATTERNS     = *.h
EXCLUDE_PATTERNS  = *-inl.h

ENABLE_PREPROCESSING = YES
MACRO_EXPANSION      = YES
EXPAND_ONLY_PREDEF   = YES
PREDEFINED           = "FLUENT_FIELD_OPTION(type, name) = TSelf& name(type);" \
                       "FLUENT_FIELD_DEFAULT(type, name, default) = TSelf& name(type);" \
                       "FLUENT_FIELD(type, name) = TSelf& name(type);" \
                       "FLUENT_FIELD_VECTOR(type, name) = TSelf& Add # name(type);"

GENERATE_LATEX = NO

# Ugly dot graphs of include dependencies are not relevant to our users
HAVE_DOT = NO
SHOW_INCLUDE_FILES = NO

# Ignore `I` and `T` prefixes of arcadia classes
IGNORE_PREFIX = I T
"""


class DoxYtError(RuntimeError):
    pass


def get_doxygen_config(output_directory):
    return DOXYGEN_CONFIG_TEMPLATE.format(
        output_directory=output_directory
    )


def iter_warnings(out):
    for line in out.split("\n"):
        if "warning:" in line or "error:" in line:
            yield line


def chdir_to_arcadia_root():
    cur = pathlib.Path(os.getcwd())
    prev = None
    while cur != prev:
        if (cur / ".arcadia.root").exists():
            os.chdir(cur)
            return
        prev = cur
        cur = cur.parent
    raise DoxYtError("Script must be run from arcadia")


def get_doxygen_path():
    expected_hash = "bff352fd49defbfff9a3dcbfc07a55a3"
    doxygen_file = pathlib.Path(tempfile.gettempdir()) / "doxygen-{}".format(expected_hash)
    if doxygen_file.exists():
        with open(doxygen_file, "rb") as inf:
            if hashlib.md5(inf.read()).hexdigest() == expected_hash:
                return doxygen_file

    print("downloading doxygen", file=sys.stderr)
    doxygen_bytes = urllib.request.urlopen("https://proxy.sandbox.yandex-team.ru/1242793942").read()
    hash
    if hashlib.md5(doxygen_bytes).hexdigest() != expected_hash:
        raise DoxYtError("downloaded doxygen has unexpected hash")
    with open(doxygen_file, "wb") as outf:
        outf.write(doxygen_bytes)
        os.fchmod(outf.fileno(), 0o755)
    return doxygen_file


def generate_documentation(output_directory):
    chdir_to_arcadia_root()

    doxygen = get_doxygen_path()
    doxygen_proc = subprocess.Popen(
        [doxygen, "-"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8",
    )

    stdout, stderr = doxygen_proc.communicate(get_doxygen_config(output_directory))
    doxy_out = stdout + stderr

    sys.stderr.write(stdout)
    sys.stderr.write(stderr)
    sys.stderr.flush()

    count = 0
    for line in iter_warnings(doxy_out):
        print(line, file=sys.stderr)
        count += 1

    print("Found {} problems".format(count))

    if doxygen_proc.returncode != 0:
        raise DoxYtError("doxygen exited with nonzero code")
    os.rename(output_directory / "html", output_directory / "c++")


def subcommand_serve(doc_dir, args):
    class HTTPServerV6(http.server.HTTPServer):
        address_family = socket.AF_INET6

    os.chdir(pathlib.Path(doc_dir) / "c++")

    server = HTTPServerV6(("::", args.port), http.server.SimpleHTTPRequestHandler)
    print("http://{}:{}/".format(socket.getfqdn(), args.port), file=sys.stderr)
    server.serve_forever()


def subcommand_pack(doc_dir, args):
    subprocess.check_call(
        ["tar", "czf", OUT_FILE, "c++"],
        cwd=doc_dir,
    )


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    serve_parser = subparsers.add_parser("serve", help="Start http server serving documentation")
    serve_parser.add_argument("port", type=int, help="port to listen", nargs="?", default=8080)
    serve_parser.set_defaults(subcommand=subcommand_serve)

    parser.set_defaults(subcommand=subcommand_pack)

    args = parser.parse_args()

    with tempfile.TemporaryDirectory() as tmpd:
        tmpd = pathlib.Path(tmpd)
        generate_documentation(tmpd)
        args.subcommand(tmpd, args)


if __name__ == '__main__':
    try:
        main()
    except DoxYtError as e:
        print(str(e).rstrip('\n'), file=sys.stderr)
        print("Error occurred, exiting...")
        exit(1)
