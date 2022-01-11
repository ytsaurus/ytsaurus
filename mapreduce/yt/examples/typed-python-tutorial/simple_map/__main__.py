# -*- coding: utf-8 -*-

import getpass

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
    # В методе prepare_operation указываются входные и выходные типы строк.
    def prepare_operation(self, context, preparer):
        preparer.input(0, type=StaffRow).output(0, type=EmailRow)

    # На вход __call__ получает одну строку входной таблицы (типа StaffRow),
    # а вернуть (yield-ом) она должна те строки,
    # которые мы хотим записать в выходную табицу (типа EmailRow).
    def __call__(self, input_row):
        yield EmailRow(
            name=input_row.name,
            email=input_row.login + "@yandex-team.ru",
        )


# Очень важно использовать конструкцию `if __name__ == "__main__"'
# в скриптах запускающих операции, без неё операции будут падать со странными ошибками.
if __name__ == "__main__":

    # Говорим библиотеке что мы будем работать с кластером freud.
    client = yt.wrapper.YtClient(proxy="freud")

    # Выходная таблица у нас будет лежать в tmp и содержать имя текущего пользователя.
    output_table = "//tmp/{}-pytutorial-emails".format(getpass.getuser())

    client.run_map(
        ComputeEmailsMapper(),
        source_table="//home/dev/typed-py-tutorial/staff_unsorted",
        destination_table=output_table,
    )

    print("Output table: https://yt.yandex-team.ru/freud/#page=navigation&offsetMode=row&path={0}".format(output_table))
