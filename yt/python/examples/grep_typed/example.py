# -*- coding: utf-8 -*-

import yt.wrapper
import yt.wrapper.schema as schema

import copy
import getpass
import os
import re
import typing


@yt.wrapper.yt_dataclass
class GrepRow:
    # В этом поле приезжает строка, по которой мы фильтруем.
    target: typing.Optional[bytes]
    # В этом поле будут приезжать все остальные колонки.
    other: schema.OtherColumns


@yt.wrapper.yt_dataclass
class StaffRow:
    # Мы хотим фильтровать по полю name типа str.
    # Это возможно, несмотря на то, что в GrepRow поле target имеет тип bytes.
    # В джобу приедет строка, закодированная в UTF-8.
    name: str
    login: str
    uid: int


class GrepMapper(yt.wrapper.TypedJob):
    def __init__(self, column, regexp):
        self._re = re.compile(regexp)
        self._column = column

    def prepare_operation(self, context, preparer):
        # Нужно заменить колонку self._column на "target" в выходной схеме.
        schema = copy.deepcopy(context.get_input_schemas()[0])
        for column in schema.columns:
            if column.name == self._column:
                column.name = "target"
                break
        else:
            assert False, "Column {} not found in input schema".format(self._column)

        # С помощью column_renaming мы переименовываем входную колонку так, чтобы
        # её название сматчилось с именем поля в GrepRow.
        preparer.inputs(
            range(context.get_input_count()), type=GrepRow, column_renaming={self._column: "target"}
        ).output(0, type=GrepRow, schema=schema)

    def __call__(self, row):
        if self._re.search(row.target):
            yield row


if __name__ == "__main__":
    # You need to set up cluster address in YT_PROXY environment variable.
    cluster = os.getenv("YT_PROXY")
    if cluster is None or cluster == "":
        raise RuntimeError("Environment variable YT_PROXY is empty")
    client = yt.wrapper.YtClient(cluster)

    input_table = "//home/tutorial/staff_unsorted_schematized"
    output_table = "//tmp/{}-pytutorial-emails".format(getpass.getuser())

    client.run_map(
        GrepMapper("name", rb"^robot-\w+$"),
        source_table=input_table,
        destination_table=output_table,
    )

    ui_url = os.getenv("YT_UI_URL")
    print(f"Output table: {ui_url}/#page=navigation&offsetMode=row&path={output_table}")
