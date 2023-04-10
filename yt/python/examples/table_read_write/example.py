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

    table = "//tmp/{}-read-write".format(getpass.getuser())

    # Просто пишем данные в таблицу, если таблица существует, её перезапишут.
    client.write_table(
        table,
        [
            {"english": "one", "russian": "один"},
            {"english": "two", "russian": "два"},
        ],
    )

    # Дописываем данные в конец таблицы, придётся поступить хитрее.
    # Используем класс TablePath и его опцию append.
    client.write_table(
        yt.wrapper.TablePath(table, append=True),
        [
            {"english": "three", "russian": "три"},
        ],
    )

    # Читаем всю таблицу.
    print("*** ALL TABLE ***")
    for row in client.read_table(table):
        print("english:", row["english"], "; russian:", row["russian"])
    print("*****************")
    print("")

    # Читаем первые 2 строки таблицы.
    print("*** FIRST TWO ROWS ***")
    for row in client.read_table(
        yt.wrapper.TablePath(table, start_index=0, end_index=2)  # читаем с 0й по 2ю строки, 2я строка невключительно
    ):
        print("english:", row["english"], "; russian:", row["russian"])
    print("*****************")
    print("")

    #  Если мы отсортируем таблицу, то можно будет читать записи по ключам.
    client.run_sort(table, sort_by=["english"])

    # И читаем запись по одному ключу.
    print("*** EXACT KEY ***")
    for row in client.read_table(
        yt.wrapper.TablePath(
            table,
            exact_key=["three"]  # В качестве ключа передаём список значений ключевых колонок
            # (тех колонок по которым отсортирована таблица).
            # Тут у нас простой случай, одна ключевая колонка, но их может быть больше.
        )
    ):
        print("english:", row["english"], "; russian:", row["russian"])
    print("*****************")
    print("")


if __name__ == "__main__":
    main()
