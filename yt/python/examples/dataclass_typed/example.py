# -*- coding: utf-8 -*-

import getpass

import yt.wrapper
import yt.wrapper.schema as schema
import typing
import os


@yt.wrapper.yt_dataclass
class Position:
    lat: float
    lon: float


@yt.wrapper.yt_dataclass
class Ride:
    # Мы используем типы из yt.wrapper.schema, когда хотим подсказать,
    # как правильно выводить тип для данной колонки в схеме.
    id: schema.Uint64

    # Для составных типов используются типы из модуля typing.
    # А в python версии >= 3.9 можно писать прямо list[Position].
    track: typing.List[Position]

    # OtherColumns позволяет несхематизированно читать и писать произвольные
    # колонки. Указание подобного поля делает выводимую схему нестрогой.
    other: schema.OtherColumns = None


# Поддерживается наследование.
@yt.wrapper.yt_dataclass
class TaxiRide(Ride):
    # Для многих полей имеет смысл использовать typing.Optional.
    # С помощью синтаксиса `= ...` вы можете указывать для поля
    # значение по умолчанию.
    # Обратите внимание: поля со значением по умолчанию должны идти в конце.
    cost: typing.Optional[int] = None

    # Специальные типы из yt.wrapper.schema позволяют
    # задать поле, в котором хранится произвольный
    # валидный yson.
    info: typing.Optional[schema.YsonBytes] = None


if __name__ == "__main__":
    # You need to set up cluster address in YT_PROXY environment variable.
    cluster = os.getenv("YT_PROXY")
    if cluster is None or cluster == "":
        raise RuntimeError("Environment variable YT_PROXY is empty")
    client = yt.wrapper.YtClient(cluster)

    table = "//tmp/{}-dataclass".format(getpass.getuser())
    client.remove(table, force=True)

    taxi_ride = TaxiRide(
        id=101,
        track=[
            Position(lat=7.0, lon=9.0),
            Position(lat=8.0, lon=10.0),
        ],
        cost=550,
        # В YsonBytes поле может лежать любой валидный YSON.
        info=schema.YsonBytes(b'["Some";"useful";"info"]'),
    )

    # Просто пишем данные в таблицу, схема выведется автоматически.
    client.write_table_structured(table, TaxiRide, [taxi_ride])

    # Читать таблицу не обязательно с помощью того же класса,
    # единственное требование ­— это совместимость схемы и датакласса.
    row_iterator = client.read_table_structured(table, Ride)

    print("*** First read ***")
    for taxi_ride in row_iterator:
        # Датаклассы поддерживают человекочитаемый __str__.
        print("  {}".format(taxi_ride))

    ride = Ride(
        id=100,
        track=[
            Position(lat=14.0, lon=12.0),
            Position(lat=14.0, lon=13.0),
        ],
        # OtherColumns конструируется от байтовой YSON-строки с мапой
        # или просто от словаря.
        other=schema.OtherColumns({"cost": 340}),
    )

    # Писать таблицу тоже можно с помощью другого класса.
    table_with_append = yt.wrapper.TablePath(table, append=True)
    client.write_table_structured(table_with_append, Ride, [ride])

    # Прочитаем, что у нас получилось.
    print("\n*** Second read ***")
    for taxi_ride in client.read_table_structured(table, TaxiRide):
        print("  {}".format(taxi_ride))
