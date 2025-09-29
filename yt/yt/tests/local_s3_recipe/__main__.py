from library.python.testing.recipe import declare_recipe

from yt.test_helpers import get_source_root, get_tests_sandbox, yatest_common, wait

import subprocess
import os
import stat
from typing import List

import boto3
import requests


################################################################################


S3_PID_FILE_NAME = "s3_pid"

S3_CONFIG = """
s3proxy.authorization=aws-v2-or-v4
s3proxy.endpoint={}
s3proxy.identity={}
s3proxy.credential={}
s3proxy.ignore-unknown-headers=true
jclouds.provider=filesystem-nio2
jclouds.filesystem.basedir={}
jclouds.identity=
jclouds.credential=
"""


def download_s3_proxy(path):
    if os.path.exists(path):
        return

    S3_PROXY_URL = "https://github.com/gaul/s3proxy/releases/download/s3proxy-2.6.0/s3proxy"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as out_file:
        out_file.write(requests.get(S3_PROXY_URL, stream=True).content)
        os.chmod(path, os.stat(path).st_mode | stat.S_IEXEC)


def get_env_value(key):
    value = os.environ[key]
    if value is None:
        raise RuntimeError(f"Environment variable {key} is required to be set to start s3proxy")
    return value


def wait_for_s3_start():
    def check_s3_reachable():
        s3_client = None
        try:
            s3_client = boto3.client("s3")
            s3_client.list_buckets()
            return True
        except:
            return False
        finally:
            if s3_client:
                s3_client.close()

    wait(check_s3_reachable, error_message="s3proxy did not start", timeout=20)


def start(_: List[str]) -> None:
    base_path = os.path.join(get_source_root(), "yt/yt/tests/local_s3_recipe")

    # Download S3Proxy binary to be used in the recipe.
    # TODO(pavel-bash): when it's added to the contrib or to the public S3, change the approach here.
    s3_binary_path = os.path.join(base_path, "bin", "s3proxy")
    download_s3_proxy(s3_binary_path)

    # Retrieve the necessary environment variables' values.
    endpoint_url = get_env_value("AWS_ENDPOINT_URL")
    access_key_id = get_env_value("AWS_ACCESS_KEY_ID")
    secret_access_key = get_env_value("AWS_SECRET_ACCESS_KEY")

    # Directory for S3 data.
    tmp_path = os.path.join(get_tests_sandbox(), "s3_runtime")
    os.makedirs(tmp_path, exist_ok=True)

    # Create a configuration file.
    s3_conf_path = os.path.join(get_tests_sandbox(), "s3proxy.conf")
    with open(s3_conf_path, "w") as s3_conf_file:
        s3_conf_file.write(S3_CONFIG.format(
            endpoint_url,
            access_key_id,
            secret_access_key,
            tmp_path,
        ))

    # Declare an S3 log file.
    s3_log_file_path = os.path.join(yatest_common.output_path(), "s3_log.log")
    s3_log_file = open(s3_log_file_path, mode="w")

    # Launch S3.
    s3_args = [
        s3_binary_path,
        "--properties",
        s3_conf_path,
    ]
    s3 = subprocess.Popen(s3_args, stdout=s3_log_file, text=True, env={"LOG_LEVEL": "debug"})
    with open(S3_PID_FILE_NAME, "w") as f:
        print(s3.pid, file=f)

    wait_for_s3_start()


def stop(_: List[str]) -> None:
    if not os.path.exists(S3_PID_FILE_NAME):
        return
    with open(S3_PID_FILE_NAME, "r") as f:
        pid = int(f.readline())

    try:
        os.kill(pid, 0)
    except OSError:
        return


################################################################################


if __name__ == "__main__":
    declare_recipe(start, stop)
