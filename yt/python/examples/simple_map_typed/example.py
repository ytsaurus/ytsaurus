# -*- coding: utf-8 -*-

import getpass
import os
import typing

import yt.wrapper


@yt.wrapper.yt_dataclass
class StaffRow:
    name: str
    login: str
    uid: int


@yt.wrapper.yt_dataclass
class EmailRow:
    name: str
    email: str


# Any job class (including Mapper) is inherited from TypedJob.
class ComputeEmailsMapper(yt.wrapper.TypedJob):
    # The `__call__` method receives one row of the input table (of type StaffRow) to the input.
    # and it should return (using yield) the rows that we want to write to the output table (of type EmailRow).
    # Type hints are needed for the API to infer the types of input and output rows.
    # These types can also be specified by overriding the `prepare_operation` method.
    def __call__(self, input_row: StaffRow) -> typing.Iterable[EmailRow]:
        yield EmailRow(
            name=input_row.name,
            email=input_row.login + "@yandex-team.ru",
        )


def main():
    # You need to set up cluster address in YT_PROXY environment variable.
    cluster = os.getenv("YT_PROXY")
    if cluster is None or cluster == "":
        raise RuntimeError("Environment variable YT_PROXY is empty")
    client = yt.wrapper.YtClient(cluster)

    # Our output table will be located in tmp and will contain the name of the current user.
    output_table = "//tmp/{}-pytutorial-typed-map-emails".format(getpass.getuser())

    client.run_map(
        ComputeEmailsMapper(),
        source_table="//home/tutorial/staff_unsorted_schematized",
        destination_table=output_table,
    )

    ui_url = os.getenv("YT_UI_URL")
    print(f"Output table: {ui_url}/#page=navigation&offsetMode=row&path={output_table}")


# It is very important to use the `if __name__ == "__main__"` construct
# in scripts that launch operations; otherwise operations will fail with unusual errors.
if __name__ == "__main__":
    main()
