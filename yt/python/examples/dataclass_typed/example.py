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
    # We use types from `yt.wrapper.schema` when we want to indicate
    # as hints for column type in schema to infer column type.
    id: schema.Uint64

    # Use types from typing module for composite types.
    # Write `list[Position]` in python version >= 3.9
    track: typing.List[Position]

    # `OtherColumns` allows to read and write random columns in a non-schematic way.
    # When such field is specified than output schema becames no strict.
    other: schema.OtherColumns = None


# Inheritance is supported.
@yt.wrapper.yt_dataclass
class TaxiRide(Ride):
    # Using of `typing.Optional` can be recommended for many fields.
    # Use syntax `= ...` to specify default value for the field.
    # NB: fields with default values should be put to the end.
    cost: typing.Optional[int] = None

    # Specific types of `yt.wrapper.schema` allow
    # setting a field storing random valid yson.
    info: typing.Optional[schema.YsonBytes] = None


def main():
    # You should set up cluster address in YT_PROXY environment variable.
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
        # Any valid Yson can be stored in the YsonBytes field.
        info=schema.YsonBytes(b'["Some";"useful";"info"]'),
    )

    # Just write data to the table and the schema will be inferred automatically
    client.write_table_structured(table, TaxiRide, [taxi_ride])

    # You don't need reading the table using the same class
    # just be sure that the schema and the dataclass are compatible.
    row_iterator = client.read_table_structured(table, Ride)

    print("*** First read ***")
    for taxi_ride in row_iterator:
        # Dataclasses can be used with a human-readable `__str__`.
        print("  {}".format(taxi_ride))

    ride = Ride(
        id=100,
        track=[
            Position(lat=14.0, lon=12.0),
            Position(lat=14.0, lon=13.0),
        ],
        # `OtherColumns` is constructed from a byte YSON string with a map
        # or simply from a dictionary.
        other=schema.OtherColumns({"cost": 340}),
    )

    # You can write to the table using another class also.
    table_with_append = yt.wrapper.TablePath(table, append=True)
    client.write_table_structured(table_with_append, Ride, [ride])

    # Reading the results.
    print("\n*** Second read ***")
    for taxi_ride in client.read_table_structured(table, TaxiRide):
        print("  {}".format(taxi_ride))

    # Or you can infer the datalcass automatically.
    print("\n*** Third read ***")
    for taxi_ride_auto in client.read_table_structured(table):
        print("  {}".format(taxi_ride_auto))


if __name__ == "__main__":
    main()
