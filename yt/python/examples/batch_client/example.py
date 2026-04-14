# -*- coding: utf-8 -*-

import getpass
import os

import yt.wrapper


def main():
    # You need to set up cluster address in YT_PROXY environment variable.
    cluster = os.getenv("YT_PROXY")
    if cluster is None or cluster == "":
        raise RuntimeError("Environment variable YT_PROXY is empty")

    client = yt.wrapper.YtClient(cluster)

    # Create a batch client.
    batch_client = client.create_batch_client()

    # Add requests to the batch. Just adding doesn`t mean that request will be executed immediately.
    # The whole batch will be executed after calling of the `commit_batch` method.
    # Result: special object is returned. We can get a result of request execution from the returned object later.
    doc_title_exists_result = batch_client.exists("//home/tutorial/doc_title")
    unexisting_table_exists_result = batch_client.exists("//home/tutorial/unexisting_table")

    output_table_name = "//tmp/{}-pytutorial-batch-client".format(getpass.getuser())
    create_result = batch_client.create("table", output_table_name)

    # Executing of the accumulated requests batch.
    batch_client.commit_batch()

    # After requests execution you can have several useful methods:
    #  - `is_ok()` — checks if was the request successfully executed or failed.
    #  - `get_result()` — returns the result of executed request. Returns "None" for failed requests.
    #  - `get_error()` — returns an error for failed requests.

    print(doc_title_exists_result.get_result())
    print(unexisting_table_exists_result.get_result())

    if create_result.is_ok():
        print("Table was created successfully.")
    else:
        # If you run this script twice a row, you`ll get an error for the second
        # time because of the table is already exists.
        print("Cannot create table {table} error: {error}.".format(
            table=output_table_name, error=create_result.get_error()
        ))


if __name__ == "__main__":
    main()
