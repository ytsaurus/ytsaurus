import os
import sys

def main():
    build_path = sys.argv[1]
    proto_path = sys.argv[2]
        
    def copy_dir(dir_path):
        if not dir_path:
            return
        copy_dir(os.path.dirname(dir_path))

        dir_full_path = os.path.join(os.getcwd(), dir_path)
        if not os.path.exists(dir_full_path):
            os.mkdir(dir_full_path)
        with open(os.path.join(dir_path, "__init__.py"), "w"):
            pass

    def copy(pb_path):
        dest_path = os.path.join("proto", pb_path)
        copy_dir(os.path.dirname(dest_path))

        with open(os.path.join(build_path, pb_path)) as fin:
            with open(os.path.join(os.getcwd(), dest_path), "w") as fout:
                for line in fin:
                    fout.write(line
                        .replace("import yt.", "import proto.yt.")
                        .replace("from yt.", "from proto.yt.")
                        .replace("import yp.", "import proto.yp.")
                        .replace("from yp.", "from proto.yp."))

    for suffix in ("_pb2.py", "_pb2_grpc.py"):
        copy(proto_path + suffix)

if __name__ == "__main__":
    main()


