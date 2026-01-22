# -*- coding: utf-8 -*-

import getpass
import os

import yt.wrapper


@yt.wrapper.yt_dataclass
class TranslationRow:
    english: str
    russian: str


def main():
    # You need to set up cluster address in YT_PROXY environment variable.
    cluster = os.getenv("YT_PROXY")
    if cluster is None or cluster == "":
        raise RuntimeError("Environment variable YT_PROXY is empty")
    client = yt.wrapper.YtClient(cluster)

    table = "//tmp/{}-read-write-typed".format(getpass.getuser())

    client.remove(table, force=True)

    # Writing data to the table. The table will be overwritten if it exists already.
    client.write_table_structured(
        table,
        TranslationRow,
        [
            TranslationRow(english="one", russian="один"),
            TranslationRow(english="two", russian="два"),
        ],
    )

    # Writing data to the end of the table. We need to do something unusual.
    # Use the `TablePath` class and its append option.
    client.write_table_structured(
        yt.wrapper.TablePath(table, append=True),
        TranslationRow,
        [
            TranslationRow(english="three", russian="три"),
        ],
    )

    # Reading the entire table.
    print("*** ALL TABLE ***")
    for row in client.read_table_structured(table, TranslationRow):
        print("english: {}; russian: {}".format(row.english, row.russian))
    print("*****************\n")

    # Reading the first 2 table rows.
    print("*** FIRST TWO ROWS ***")
    for row in client.read_table_structured(
        # reading from zero to the second row. Do not read the 2nd row.
        yt.wrapper.TablePath(table, start_index=0, end_index=2),
        TranslationRow,
    ):
        print("english: {}; russian: {}".format(row.english, row.russian))
    print("*****************\n")

    # The `with_context` method of the iterator helps to get row indices.
    print("*** ALL ROWS WITH CONTEXT ***")
    for row, ctx in client.read_table_structured(table, TranslationRow).with_context():
        print("Row{}: english: {}; russian: {}".format(ctx.get_row_index(), row.english, row.russian))
    print("*****************\n")

    # We can read records by keys if we sort the table.
    client.run_sort(table, sort_by=["english"])

    # Reading a record using one key.
    print("*** EXACT KEY ***")
    for row in client.read_table_structured(
        yt.wrapper.TablePath(
            table,
            exact_key=["three"]  # pass a list of key columns values as the key
            # (that columns by which the table was sorted).
            # We have a simple case with one key column. But there can be more of them.
        ),
        TranslationRow,
    ):
        print("english: {}; russian: {}".format(row.english, row.russian))
    print("*****************\n")


if __name__ == "__main__":
    main()
