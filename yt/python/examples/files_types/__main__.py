# -*- coding: utf-8 -*-

import getpass

import yt.wrapper


@yt.wrapper.yt_dataclass
class Row:
    x: int


class Mapper(yt.wrapper.TypedJob):
    def prepare_operation(self, context, preparer):
        preparer.input(0, Row).output(0, Row)

    def __call__(self, row):
        assert open("cypress_file").read() == "cypress file"
        assert open("pytutorial_local_file").read() == "local file"
        assert open("other_local_file").read() == "local file"
        yield row


def main():
    client = yt.wrapper.YtClient(proxy="freud")

    path = "//tmp/{}-pytutorial-files".format(getpass.getuser())
    client.create("map_node", path, force=True)

    local_path = "/tmp/pytutorial_local_file"
    with open(local_path, "w") as fout:
        fout.write("local file")

    cypress_path = yt.wrapper.ypath_join(path, "cypress_file")

    # Записывать в файл можно из потока (бинарного!).
    with open(local_path, "rb") as f:
        client.write_file(cypress_path, f)
    assert client.read_file(cypress_path, length=5).read() == b"local"
    assert client.read_file(cypress_path, offset=6).read() == b"file"

    # Записывать в файл можно только bytes.
    client.write_file(cypress_path, b"cypress file")
    assert client.read_file(cypress_path, length=7).read() == b"cypress"
    assert client.read_file(cypress_path, offset=8).read() == b"file"

    client.write_table_structured(yt.wrapper.ypath_join(path, "input_table"), Row, [Row(x=1)])

    # В операции также можно передавать файлы.
    # В параметре yt_files передаются пути до файлов, уже загруженных в Кипарис.
    # В параметре local_files передаются пути до локальных файлов
    # (можно оборачивать в LocalFile и указывать путь, по которому файл будет виден в джобе).
    client.run_map(
        Mapper(),
        yt.wrapper.ypath_join(path, "input_table"),
        yt.wrapper.ypath_join(path, "output_table"),
        yt_files=[cypress_path],
        local_files=[
            local_path,
            yt.wrapper.LocalFile(local_path, file_name="other_local_file"),
        ],
    )


if __name__ == "__main__":
    main()
