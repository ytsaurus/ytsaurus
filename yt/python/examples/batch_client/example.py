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

    # Создаём батч-клиент.
    batch_client = client.create_batch_client()

    # Добавляем запросы в пачку, добавление запроса в этом месте не приводит к его немедленному выполнению.
    # Вся пачка выполняется после того, как будет позван метод commit_batch.
    # В качестве результата нам возвращается специальный объект, из которого можно будет позже получить результат
    # выполнения запроса.
    doc_title_exists_result = batch_client.exists("//home/dev/tutorial/doc_title")
    unexisting_table_exists_result = batch_client.exists("//home/dev/tutorial/unexisting_table")

    output_table_name = "//tmp/{}-pytutorial-batch-client".format(getpass.getuser())
    create_result = batch_client.create("table", output_table_name)

    # Выполняем накопленную пачку запросов.
    batch_client.commit_batch()

    # После выполнения запросов, есть несколько полезных методов.
    #  - is_ok() -- проверяет успешно ли был выполненен запрос
    #  - get_result() -- возвращает результат, если запрос был выполнен успешно (для неуспешно выполненых возвращает None)
    #  - get_error() -- возвращает ошибку для неудачно выполненых запросов

    print(doc_title_exists_result.get_result())
    print(unexisting_table_exists_result.get_result())

    if create_result.is_ok():
        print("Table was created successfully.")
    else:
        # Если запустить этот скрипт два раза подряд, то во второй раз создание таблицы завершится с ошибкой,
        # что таблица уже существует.
        print("Cannot create table {table} error: {error}.".format(
            table=output_table_name, error=create_result.get_error()
        ))


if __name__ == "__main__":
    main()
