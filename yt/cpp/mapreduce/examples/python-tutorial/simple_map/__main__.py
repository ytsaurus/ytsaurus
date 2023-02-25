# -*- coding: utf-8 -*-

import getpass

import yt.wrapper


# Маппер это обычная функция-генератор. На вход она получает одну строку входной таблицы,
# вернуть она должна те строки, которые мы хотим записать в выходную табицу.
def compute_emails_mapper(input_row):
    output_row = {}

    output_row["name"] = input_row["name"]
    output_row["email"] = input_row["login"] + "@yandex-team.ru"

    yield output_row


# Очень важно использовать конструкцию `if __name__ == "__main__"'
# в скриптах запускающих операции, без неё операции будут падать со странными ошибками.
if __name__ == "__main__":

    # Говорим библиотеке что мы будем работать с кластером freud.
    client = yt.wrapper.YtClient(proxy="freud")

    # Выходная таблица у нас будет лежать в tmp и содержать имя текущего пользователя.
    output_table = "//tmp/{}-pytutorial-emails".format(getpass.getuser())

    client.run_map(
        compute_emails_mapper, source_table="//home/dev/tutorial/staff_unsorted", destination_table=output_table
    )

    print(("Output table: https://yt.yandex-team.ru/freud/#page=navigation&offsetMode=row&path={0}".format(output_table)))
