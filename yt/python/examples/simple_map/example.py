# -*- coding: utf-8 -*-

import getpass
import os

import yt.wrapper


# A mapper is a regular generator function. It gets one row from the input table as input
# and should return the rows which we want to write to the output table.
def compute_emails_mapper(input_row):
    output_row = {}

    output_row["name"] = input_row["name"]
    output_row["email"] = input_row["login"] + "@yandex-team.ru"

    yield output_row


def main():
    # You should set up cluster address in YT_PROXY environment variable.
    cluster = os.getenv("YT_PROXY")
    if cluster is None or cluster == "":
        raise RuntimeError("Environment variable YT_PROXY is empty")
    client = yt.wrapper.YtClient(cluster)

    # Our output table will be located in tmp and will contain a name of the current user.
    output_table = "//tmp/{}-pytutorial-map-emails".format(getpass.getuser())

    client.run_map(
        compute_emails_mapper, source_table="//home/tutorial/staff_unsorted", destination_table=output_table
    )

    ui_url = os.getenv("YT_UI_URL")
    print(f"Output table: {ui_url}/#page=navigation&offsetMode=row&path={output_table}")


# It is very important to use the `if __name__ == "__main__"` construct
# in scripts launching operations; operations will fail with unexpected errors without it.
if __name__ == "__main__":
    main()
