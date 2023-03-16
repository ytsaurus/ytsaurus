# -*- coding: utf-8 -*-

import getpass
import typing

import yt.wrapper
from yt.wrapper.schema import TableSchema, Uint64

import yandex.type_info.typing as ti
# для Аркадийной сборки предпочтительно использовать
# yt.type_info.typing (см https://yt.yandex-team.ru/docs/api/python/start#faq)


@yt.wrapper.yt_dataclass
class Meeting:
    id: Uint64
    room_id: typing.Optional[int] = None


@yt.wrapper.yt_dataclass
class StaffEntry:
    id: Uint64
    name: str
    meetings: typing.List[Meeting]


if __name__ == "__main__":
    client = yt.wrapper.YtClient(proxy="freud")

    table = "//tmp/{}-table-schema".format(getpass.getuser())
    client.remove(table, force=True)

    rows = [
        StaffEntry(id=12, name="Leonid", meetings=[]),
        StaffEntry(id=1, name="Alexey", meetings=[Meeting(id=6)]),
    ]

    # Самый правильный способ создать таблицу со схемой — просто начать
    # писать в неё датакласс.
    client.write_table_structured(table, StaffEntry, rows)

    # Напечатаем выведенную схему.
    schema = TableSchema.from_yson_type(client.get(yt.wrapper.ypath_join(table, "@schema")))
    print("***** Inferred schema *****")
    print(schema)

    # Может потребоваться создать таблицу с сортированной схемой.
    # В таком случае стоит использовать метод build_schema_sorted_by().
    sorted_table = "//tmp/{}-table-schema-sorted".format(getpass.getuser())
    sorted_schema = schema.build_schema_sorted_by(["name"])
    client.create("table", sorted_table, attributes={"schema": sorted_schema})

    client.write_table_structured(table, StaffEntry, sorted(rows, key=lambda r: r.name))

    # Создавать схему можно и вручную с помощью builder-интерфейса.
    # Обратите внимание, что типы задаются с помощью библиотеки type_info,
    # https://a.yandex-team.ru/arc_vcs/library/cpp/type_info/docs/main.md.
    #
    # NB. Типу str из Python 3 соответствует ti.Utf8, а bytes — ti.String.
    handmade_schema = (
        TableSchema()
        .add_column("id", ti.Uint64)
        .add_column("name", ti.Utf8)
        .add_column(
            "meetings",
            ti.List[
                ti.Struct[
                    "id" : ti.Uint64,
                    "room_id" : ti.Optional[ti.Int64],
                ]
            ],
        )
    )

    assert handmade_schema == schema
    print("***** Hand-made schema *****")
    print(handmade_schema)
