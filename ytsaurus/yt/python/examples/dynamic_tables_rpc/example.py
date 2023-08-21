# -*- coding: utf-8 -*-

import yt.wrapper as yt

import getpass
import os


def main():
    # You need to set up cluster address in YT_PROXY environment variable.
    cluster = os.getenv("YT_PROXY")
    if cluster is None or cluster == "":
        raise RuntimeError("Environment variable YT_PROXY is empty")
    # Создаём rpc-клиент.
    client = yt.wrapper.YtClient(cluster, config={"backend": "rpc"})

    schema = [
        {"name": "x", "type": "int64", "sort_order": "ascending"},
        {"name": "y", "type": "int64"},
        {"name": "z", "type": "int64"},
    ]
    table = "//tmp/{}-pytutorial-dynamic-tables-rpc".format(getpass.getuser())
    client.create("table", table, force=True, attributes={"schema": schema, "dynamic": True})

    # Монтируем свежесозданную динамическую таблицу и добавляем в неё строку.
    client.mount_table(table, sync=True)
    client.insert_rows(table, [{"x": 0, "y": 99}])

    # Создаём таблетную транзакцию для транзакционной работы с таблицей.
    with client.Transaction(type="tablet"):
        # Получаем строку с данным ключом
        rows = list(client.lookup_rows(table, [{"x": 0}]))
        if len(rows) == 1 and rows[0]["y"] <= 100:
            rows[0]["y"] += 1
            # Обновляем строку.
            client.insert_rows(table, rows)

    # Выбираем все строки из таблицы и печатаем их.
    print(list(client.select_rows("* from [{}]".format(table))))


if __name__ == "__main__":
    main()
