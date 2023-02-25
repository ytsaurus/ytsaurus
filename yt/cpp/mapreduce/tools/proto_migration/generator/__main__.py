# coding=utf-8

import argparse
import logging
import subprocess
import os.path
import re

logger = logging.getLogger(__name__)

PROJECT_NAME = "root_lib"

PROTO_FILE_NAME = "main.proto"

YA_MAKE_TEMPLATE = """
LIBRARY()

SRCS({proto_file})

PEERDIR(
    {peerdir}
)

END()
"""

BLACKLIST = [
    "mlp/mail/aspam/experiments/MLP_316",
    "mlp/mail/ind/demo/types/proto",
    "mlp/mail/smartobject/types/proto",
]


def get_project(path, arcadia_root):
    dir_path, rel_path = os.path.split(path)
    while dir_path != "":
        yamake_path = os.path.join(arcadia_root, dir_path, "ya.make")
        if os.path.exists(yamake_path):
            with open(yamake_path) as f:
                yamake = f.read()
            if rel_path in yamake:
                if "PY2_LIBRARY(" in yamake or "PY3_LIBRARY(" in yamake or "PY23_LIBRARY(" in yamake:
                    logger.warning("Ignoring python library %s", yamake_path)
                    return None
                elif "PROGRAM(" in yamake:
                    logger.warning("Ignoring program %s", yamake_path)
                    return None
                else:
                    return dir_path
        dir_path, dir_name = os.path.split(dir_path)
        rel_path = os.path.join(dir_name, rel_path)
    return None


PACKAGE_NAME_RE = re.compile(r"^\s*package ((?:\w|\.)+);", flags=re.MULTILINE)
MESSAGE_DEF_RE = re.compile(r"^message (\w+)", flags=re.MULTILINE)


def extract_full_message_names(proto_file):
    with open(proto_file) as f:
        contents = f.read()
    package_names = re.findall(PACKAGE_NAME_RE, contents)
    if len(package_names) == 0:
        package_name = ""
    else:
        package_name = package_names[0]
    result = []
    for message_name in re.findall(MESSAGE_DEF_RE, contents):
        if package_name:
            result.append("{}.{}".format(package_name, message_name))
        else:
            result.append(message_name)
    return result


def generate_proto_file(path, files, arcadia_root):
    full_message_names = []
    for f in files:
        full_message_names += extract_full_message_names(os.path.join(arcadia_root, f))
    with open(path, "w") as f:
        f.write("package NYT.NProtoMigration;\n\n")
        for file_name in files:
            f.write('import "{}";\n'.format(file_name))
        f.write("\nmessage TRoot\n{\n")
        for i, full_name in enumerate(full_message_names):
            f.write("    optional {full_name} {message_name}_{i} = {i};\n".format(
                full_name=full_name,
                message_name=full_name.split('.')[-1],
                i=i + 1),
            )
        f.write("}\n")


def gen_lib(args):
    found = subprocess.check_output(
        r"ya grep -l -f .*\.proto$ \WSERIALIZATION_YT\W --remote --no-junk -m 10000".split(),
        cwd=args.arcadia_root,
    )
    files = [line.decode("utf-8") for line in found.split()]
    if args.num_files is not None:
        files = files[:args.num_files]
    projects = set()
    good_files = []
    for f in files:
        project = get_project(f, args.arcadia_root)
        if project is None:
            logger.warning("Failed to find project path for file %s", f)
        elif any(blacklisted in f for blacklisted in BLACKLIST):
            logger.warning("File %s is blacklisted", f)
        else:
            projects.add(project)
            good_files.append(f)
    os.makedirs(args.project_dir, exist_ok=True)
    with open(os.path.join(args.project_dir, "ya.make"), "w") as f:
        f.write(YA_MAKE_TEMPLATE.format(
            peerdir="\n    ".join(projects),
            proto_file=PROTO_FILE_NAME,
        ))
    generate_proto_file(os.path.join(args.project_dir, PROTO_FILE_NAME), good_files, args.arcadia_root)


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title="commands", dest="cmd", required=True)
    gen_lib_parser = subparsers.add_parser(
        "gen-lib",
        help="generate LIBRARY with a message containing as fields "
             "all the messages containing SERIALIZATION_YT",
    )
    gen_lib_parser.add_argument("--arcadia-root", required=True)
    gen_lib_parser.add_argument("--project-dir", default=PROJECT_NAME)
    gen_lib_parser.add_argument("--num-files", type=int, help="How many files to generate usages for")
    gen_lib_parser.set_defaults(func=gen_lib)
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
