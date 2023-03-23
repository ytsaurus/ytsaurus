# -*- coding: utf-8 -*-

import getpass

import yt.wrapper


@yt.wrapper.yt_dataclass
class TranslationRow:
    english: str
    russian: str


if __name__ == "__main__":
    client = yt.wrapper.YtClient(proxy="freud")

    table = "//tmp/{}-read-write".format(getpass.getuser())

    # Просто пишем данные в таблицу, если таблица существует, её перезапишут.
    client.write_table_structured(
        table,
        TranslationRow,
        [
            TranslationRow(english="one", russian="один"),
            TranslationRow(english="two", russian="два"),
        ],
    )

    # Дописываем данные в конец таблицы, придётся поступить хитрее.
    # Используем класс TablePath и его опцию append.
    client.write_table_structured(
        yt.wrapper.TablePath(table, append=True),
        TranslationRow,
        [
            TranslationRow(english="three", russian="три"),
        ],
    )

    # Читаем всю таблицу.
    print("*** ALL TABLE ***")
    for row in client.read_table_structured(table, TranslationRow):
        print("english: {}; russian: {}".format(row.english, row.russian))
    print("*****************\n")

    # Читаем первые 2 строки таблицы.
    print("*** FIRST TWO ROWS ***")
    for row in client.read_table_structured(
        # читаем с 0й по 2ю строки, 2я строка невключительно.
        yt.wrapper.TablePath(table, start_index=0, end_index=2),
        TranslationRow,
    ):
        print("english: {}; russian: {}".format(row.english, row.russian))
    print("*****************\n")

    # Метод with_context у итератора позволяет получить индексы строк.
    print("*** ALL ROWS WITH CONTEXT ***")
    for row, ctx in client.read_table_structured(table, TranslationRow).with_context():
        print("Row{}: english: {}; russian: {}".format(ctx.get_row_index(), row.english, row.russian))
    print("*****************\n")

    #  Если мы отсортируем таблицу, то можно будет читать записи по ключам.
    client.run_sort(table, sort_by=["english"])

    # И читаем запись по одному ключу.
    print("*** EXACT KEY ***")
    for row in client.read_table_structured(
        yt.wrapper.TablePath(
            table,
            exact_key=["three"]  # В качестве ключа передаём список значений ключевых колонок
            # (тех колонок по которым отсортирована таблица).
            # Тут у нас простой случай, одна ключевая колонка, но их может быть больше.
        ),
        TranslationRow,
    ):
        print("english: {}; russian: {}".format(row.english, row.russian))
    print("*****************\n")
