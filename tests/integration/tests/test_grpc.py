import imp
import os
import sys
import subprocess

from yt_env_setup import SANDBOX_ROOTDIR

TEST_DIR = os.path.join(os.path.dirname(__file__))


def which(file):
    for path in os.environ["PATH"].split(os.pathsep):
        if os.path.exists(os.path.join(path, file)):
                return os.path.join(path, file)

    return None

def test_grpc():
    sandbox_dir = os.path.join(SANDBOX_ROOTDIR, "test_grpc")
    if not os.path.exists(sandbox_dir):
        os.makedirs(sandbox_dir)

    protos_dir = os.path.join(TEST_DIR, "test_grpc_protos")

    grpc_python_path = which("grpc_python")
    assert grpc_python_path is not None

    subprocess.check_call([
        "protoc",
        "-I=" + protos_dir,
        "--python_out=" + sandbox_dir,
        os.path.join(TEST_DIR, "test_grpc_protos/hello.proto"),
        "--plugin=protoc-gen-grpc_py=" + grpc_python_path,
        "--grpc_py_out=" + sandbox_dir])

    sys.path.append(sandbox_dir)
    fp, pathname, description = imp.find_module("hello_pb2")
    hello_pb2 = imp.load_module("hello_pb2", fp, pathname, description)

    hello_pb2.HelloRequest()
    hello_pb2.HelloResponse()
    

