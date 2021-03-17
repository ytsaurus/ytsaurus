# -*- coding: utf-8 -*-

import getpass

import yt.wrapper


# Reducer это обычная функция-генератор, она принимает на вход:
#   - текущий ключ
#   - итератор по всем записям входной таблицы с данным ключом
# на выходе она должна вернуть (как и функция mapper) все записи, которые мы хотим записать в выходные таблицы.
def count_names_reducer(key, input_row_iterator):

    # В данном случае ключ у нас состоит лишь из одной колонки "name", но вообще он может состоять из нескольких колонок.
    # Читать конкретные поля ключа можно как из dict'а.
    name = key["name"]

    count = 0
    for input_row in input_row_iterator:
        count += 1

    yield {"name": name, "count": count}


if __name__ == "__main__":
    yt.wrapper.config.set_proxy("freud")

    sorted_tmp_table = "//tmp/" + getpass.getuser() + "-pytutorial-tmp"
    output_table = "//tmp/" + getpass.getuser() + "-pytutorial-name-stat"

    yt.wrapper.run_sort(
        source_table="//home/ermolovd/yt-tutorial/staff_unsorted", destination_table=sorted_tmp_table, sort_by=["name"]
    )

    yt.wrapper.run_reduce(
        count_names_reducer, source_table=sorted_tmp_table, destination_table=output_table, reduce_by=["name"]
    )

    print("Output table: https://yt.yandex-team.ru/freud/#page=navigation&offsetMode=row&path={0}".format(output_table))
