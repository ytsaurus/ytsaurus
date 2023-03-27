# -*- coding: utf-8 -*-

import getpass
import os

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
    # You need to set up cluster address in YT_PROXY environment variable.
    cluster = os.getenv("YT_PROXY")
    if cluster is None or cluster == "":
        raise RuntimeError("Environment variable YT_PROXY is empty")
    client = yt.wrapper.YtClient(cluster)

    # Выходная таблица у нас будет лежать в tmp и содержать имя текущего пользователя.
    output_table = "//tmp/{}-pytutorial-emails".format(getpass.getuser())

    client.run_map(
        compute_emails_mapper, source_table="//home/dev/tutorial/staff_unsorted", destination_table=output_table
    )

    ui_url = os.getenv("YT_UI_URL")
    print(f"Output table: {ui_url}/#page=navigation&offsetMode=row&path={output_table}")
