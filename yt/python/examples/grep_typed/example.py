# -*- coding: utf-8 -*-

import yt.wrapper
import yt.wrapper.schema as schema

import copy
import getpass
import os
import re


@yt.wrapper.yt_dataclass
class GrepRow:
    # In this field we get string for filtering.
    target: bytes
    # In this field we get all other columns.
    other: schema.OtherColumns


@yt.wrapper.yt_dataclass
class StaffRow:
    # We can filter by the field `name` of type `str` in spite of the field `target` of type `bytes` in GrepRow.
    # UTF-8 coded string will come into the job.
    name: str
    login: str
    uid: int


class GrepMapper(yt.wrapper.TypedJob):
    def __init__(self, column, regexp):
        self._re = re.compile(regexp)
        self._column = column

    def prepare_operation(self, context, preparer):
        # You should replace the column `self._column` with "target" in the output system.
        schema = copy.deepcopy(context.get_input_schemas()[0])
        for column in schema.columns:
            if column.name == self._column:
                column.name = "target"
                break
        else:
            assert False, "Column {} not found in input schema".format(self._column)

        # Use the column_renaming for renaming input column in order to match its name with field name in GrepRow.
        preparer.inputs(
            range(context.get_input_count()), type=GrepRow, column_renaming={self._column: "target"}
        ).output(0, type=GrepRow, schema=schema)

    def __call__(self, row):
        if self._re.search(row.target):
            yield row


def main():
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


if __name__ == "__main__":
    main()
