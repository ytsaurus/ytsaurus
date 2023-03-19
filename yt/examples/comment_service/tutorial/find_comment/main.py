# -*- coding: utf-8 -*-
import yt.wrapper as yt
import json
import os


def find_comment(topic_id, parent_path, client, table_path):
    # В lookup_rows требуется передать значения всех ключевых колонок
    return list(client.lookup_rows(
        "{}/topic_comments".format(table_path),
        [{"topic_id": topic_id, "parent_path": parent_path}],
    ))


def main():
    table_path = os.environ["TABLE_PATH"]
    cluster_name = os.environ["CLUSTER"]
    client = yt.YtClient(cluster_name, config={"backend": "rpc"})

    comment_info = find_comment(
        topic_id="1dd64501-4131025-562332a3-40507acc",
        parent_path="0",
        client=client, table_path=table_path,
    )
    print(json.dumps(comment_info, indent=4))


if __name__ == "__main__":
    main()

"""
Вывод программы:

[
    {
        "update_time": 100000,
        "views_count": 0,
        "parent_id": 0,
        "deleted": false,
        "comment_id": 0,
        "creation_time": 100000,
        "content": "Some comment text",
        "parent_path": "0",
        "user": "abc",
        "topic_id": "1dd64501-4131025-562332a3-40507acc"
    }
]
"""
