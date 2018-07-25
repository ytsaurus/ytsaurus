#!/usr/bin/env python

import os
import sys
import errno

def makedirp(path):
    try:
        os.makedirs(path)
    except OSError as err:
        if err.errno != errno.EEXIST:
            raise

def main():
    build_path = sys.argv[1]
    proto_path = sys.argv[2]

    def copy_dir(dir_path):
        if not dir_path:
            return
        copy_dir(os.path.dirname(dir_path))

        makedirp(os.path.join(os.getcwd(), dir_path))
        with open(os.path.join(dir_path, "__init__.py"), "w"):
            pass

    def copy(pb_path):
        # To avoid conflicts in debian packages yt & yp protos
        # are installed to copied locations.
        if pb_path.startswith("yt"):
            dest_path = os.path.join("yt_proto", pb_path)
        else:
            assert pb_path.startswith("yp")
            dest_path = os.path.join("yp_proto", pb_path)

        copy_dir(os.path.dirname(dest_path))

        with open(os.path.join(build_path, pb_path)) as fin:
            with open(os.path.join(os.getcwd(), dest_path), "w") as fout:
                for line in fin:
                    fout.write(line
                        .replace("import yt.", "import yt_proto.yt.")
                        .replace("from yt.", "from yt_proto.yt.")
                        .replace("import yp.", "import yp_proto.yp.")
                        .replace("from yp.", "from yp_proto.yp."))

    for suffix in ("_pb2.py", "_pb2_grpc.py"):
        copy(proto_path + suffix)

if __name__ == "__main__":
    main()


