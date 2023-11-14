# -*- coding: utf-8 -*-

import getpass
import os
import typing

import yt.wrapper


@yt.wrapper.yt_dataclass
class StaffRow:
    name: str
    login: str
    uid: int


@yt.wrapper.yt_dataclass
class EmailRow:
    name: str
    email: str


# Любой класс джоба (в том числе Mapper) -- это наследник TypedJob.
class ComputeEmailsMapper(yt.wrapper.TypedJob):
    # На вход __call__ получает одну строку входной таблицы (типа StaffRow),
    # а вернуть (yield-ом) она должна те строки,
    # которые мы хотим записать в выходную табицу (типа EmailRow).
    # Тайпинги нужны API для выведения типов входных и выходных строк.
    # По-другому эти типы можно указать переопределив метод prepare_operation
    def __call__(self, input_row: StaffRow) -> typing.Iterable[EmailRow]:
        yield EmailRow(
            name=input_row.name,
            email=input_row.login + "@yandex-team.ru",
        )


def main():
    # You need to set up cluster address in YT_PROXY environment variable.
    cluster = os.getenv("YT_PROXY")
    if cluster is None or cluster == "":
        raise RuntimeError("Environment variable YT_PROXY is empty")
    client = yt.wrapper.YtClient(cluster)

    # Выходная таблица у нас будет лежать в tmp и содержать имя текущего пользователя.
    output_table = "//tmp/{}-pytutorial-typed-map-emails".format(getpass.getuser())

    client.run_map(
        ComputeEmailsMapper(),
        source_table="//home/tutorial/staff_unsorted_schematized",
        destination_table=output_table,
    )

    ui_url = os.getenv("YT_UI_URL")
    print(f"Output table: {ui_url}/#page=navigation&offsetMode=row&path={output_table}")


# Очень важно использовать конструкцию `if __name__ == "__main__"'
# в скриптах запускающих операции, без неё операции будут падать со странными ошибками.
if __name__ == "__main__":
    main()
