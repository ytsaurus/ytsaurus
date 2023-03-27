# -*- coding: utf-8 -*-

import getpass
import os
import typing

import yt.wrapper
from yt.wrapper import schema


@yt.wrapper.yt_dataclass
class User:
    uid: schema.Uint64
    name: str


@yt.wrapper.yt_dataclass
class Activity:
    uid: schema.Uint64
    type: str
    url: typing.Optional[bytes] = None


@yt.wrapper.yt_dataclass
class Click:
    uid: schema.Uint64
    url: bytes


@yt.wrapper.yt_dataclass
class NamedClicks:
    name: str
    urls: typing.List[bytes]


@yt.wrapper.with_context
class Mapper(yt.wrapper.TypedJob):
    # В методе prepare_operation указываются входные и выходные типы строк.
    def prepare_operation(self, context, preparer):
        # Вместо обычных констант лучше использовать именованные,
        # но для краткости напишем так.
        preparer.input(0, type=User).input(1, type=Activity).output(0, type=User).output(1, type=Click)

    def __call__(self, row, context):
        if context.get_table_index() == 0:
            yield yt.wrapper.OutputRow(row, table_index=0)
        else:
            assert context.get_table_index() == 1
            # Выбираем только события типа "клик".
            if row.type == "click":
                assert row.url is not None
                yield yt.wrapper.OutputRow(
                    Click(uid=row.uid, url=row.url),
                    table_index=1,
                )

    def get_intermediate_stream_count(self):
        # Указываем, что промежуточные данные текут в два потока.
        return 2


class Reducer(yt.wrapper.TypedJob):
    def prepare_operation(self, context, preparer):
        # В объекте context лежит информация о количестве входных и выходных потоков,
        # их схемы и пути к таблицам.
        assert context.get_input_count() == 2
        assert context.get_output_count() == 1

        preparer.input(0, type=User).input(1, type=Click).output(0, type=NamedClicks)

    def __call__(self, rows):
        urls = []
        user_row = None
        for row, ctx in rows.with_context():
            if ctx.get_table_index() == 0:
                # Пользователи приходят в нулевой таблице.
                user_row = row
            else:
                assert ctx.get_table_index() == 1
                # А клики — в первой.
                urls.append(row.url)
        if user_row is not None:
            yield NamedClicks(name=user_row.name, urls=urls)


if __name__ == "__main__":
    # You need to set up cluster address in YT_PROXY environment variable.
    cluster = os.getenv("YT_PROXY")
    if cluster is None or cluster == "":
        raise RuntimeError("Environment variable YT_PROXY is empty")
    client = yt.wrapper.YtClient(cluster)

    users = "//tmp/{}-pytutorial-multiple-intermediate-users".format(getpass.getuser())
    activities = "//tmp/{}-pytutorial-multiple-intermediate-activities".format(getpass.getuser())
    client.remove(users, force=True)
    client.remove(activities, force=True)

    client.write_table_structured(
        users,
        User,
        [
            User(uid=123, name="Petya"),
            User(uid=124, name="Vasya"),
            User(uid=125, name="Anya"),
            User(uid=126, name="Sveta"),
        ],
    )

    client.write_table_structured(
        activities,
        Activity,
        [
            Activity(uid=123, type="click", url=b"yandex.ru"),
            Activity(uid=125, type="login"),
            Activity(uid=125, type="click", url=b"youtube.com"),
            Activity(uid=126, type="click", url=b"ya.ru"),
            Activity(uid=124, type="click", url=b"yandex.ru"),
            Activity(uid=123, type="logout"),
        ],
    )

    output_table = "//tmp/{}-pytutorial-multiple-intermediate-clicks".format(getpass.getuser())

    client.run_map_reduce(
        Mapper(),
        Reducer(),
        [users, activities],
        output_table,
        reduce_by=["uid"],
    )

    ui_url = os.getenv("YT_UI_URL")
    print(f"Output table: {ui_url}/#page=navigation&offsetMode=row&path={output_table}")
