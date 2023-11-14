# -*- coding: utf-8 -*-

import getpass
import os

import yt.wrapper

from yt.wrapper import LocalFile


def mapper(row):
    assert open("cypress_file").read() == "cypress file"
    assert open("pytutorial_local_file").read() == "local file"
    assert open("other_local_file").read() == "local file"
    yield row


def main():
    # You need to set up cluster address in YT_PROXY environment variable.
    cluster = os.getenv("YT_PROXY")
    if cluster is None or cluster == "":
        raise RuntimeError("Environment variable YT_PROXY is empty")
    client = yt.wrapper.YtClient(cluster)

    path = "//tmp/{}-pytutorial-files".format(getpass.getuser())
    client.create("map_node", path, force=True)

    local_path = "/tmp/pytutorial_local_file"
    with open(local_path, "wb") as fout:
        fout.write(b"local file")

    cypress_path = path + "/cypress_file"

    # Записывать в файл можно из потока.
    with open(local_path, "rb") as f:
        client.write_file(cypress_path, f)
    assert client.read_file(cypress_path, length=5).read() == b"local"
    assert client.read_file(cypress_path, offset=6).read() == b"file"

    # Записывать в файл можно просто строку (или bytes).
    client.write_file(cypress_path, b"cypress file")
    assert client.read_file(cypress_path, length=7).read() == b"cypress"
    assert client.read_file(cypress_path, offset=8).read() == b"file"

    client.write_table(path + "/input_table", [{"x": 1}])

    # В операции также можно передавать файлы.
    # В параметре yt_files передаются пути до файлов, уже загруженных в Кипарис.
    # В параметре local_files передаются пути до локальных файлов
    # (можно оборачивать в LocalFile и указывать путь, по которому файл будет виден в джобе).
    client.run_map(
        mapper,
        path + "/input_table",
        path + "/output_table",
        yt_files=[cypress_path],
        local_files=[local_path, LocalFile(local_path, file_name="other_local_file")],
    )


if __name__ == "__main__":
    main()
