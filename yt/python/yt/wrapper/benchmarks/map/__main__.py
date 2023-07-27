import yt.yson as yson
import yt.wrapper as yt

from yt.wrapper.schema import yt_dataclass, Uint64, Uint32, OtherColumns

import argparse
import sys
from typing import Optional


COLUMNS = [
    "VisitClickCoefGoalContextID",
    "URLClusterAlgShows",
    "VisitCurrencyID",
    "BMCategoryHits",
    "LM7",
]

DEFAULT_SRC_TABLE = "//home/dev/psushin/yt_bench/scan"


@yt_dataclass
class Row:
    VisitClickCoefGoalContextID: Optional[bytes]
    URLClusterAlgShows: Optional[Uint64]
    VisitCurrencyID: Optional[Uint32]
    BMCategoryHits: Optional[Uint32]
    LM7: Optional[float]


@yt_dataclass
class RowWithOtherColumns:
    VisitClickCoefGoalContextID: Optional[bytes]
    URLClusterAlgShows: Optional[Uint64]
    VisitCurrencyID: Optional[Uint32]
    BMCategoryHits: Optional[Uint32]
    LM7: Optional[float]
    other: OtherColumns


class SkiffMapperSimple(yt.TypedJob):
    def prepare_operation(self, context, preparer):
        schema = context.get_input_schemas()[0]
        preparer \
            .input(0, type=RowWithOtherColumns) \
            .output(0, type=RowWithOtherColumns, schema=schema)

    def __call__(self, row):
        yield row


class SkiffMapperCopy(yt.TypedJob):
    def prepare_operation(self, context, preparer):
        preparer \
            .input(0, type=Row) \
            .output(0, type=Row) # noqa

    def __call__(self, row):
        yield Row.create(
            VisitClickCoefGoalContextID=row.VisitClickCoefGoalContextID,
            URLClusterAlgShows=row.URLClusterAlgShows,
            VisitCurrencyID=row.VisitCurrencyID,
            BMCategoryHits=row.BMCategoryHits,
            LM7=row.LM7,
        )


class ClassicMapperSimple:
    def __call__(self, row):
        yield row


class ClassicMapperCopy:
    def __call__(self, row):
        yield {
            "VisitClickCoefGoalContextID": row["VisitClickCoefGoalContextID"],
            "URLClusterAlgShows": row["URLClusterAlgShows"],
            "VisitCurrencyID": row["VisitCurrencyID"],
            "BMCategoryHits": row["BMCategoryHits"],
            "LM7": row["LM7"],
        }


def info(type, value, tb):
    if hasattr(sys, 'ps1') or not sys.stderr.isatty():
        # we are in interactive mode or we don't have a tty-like
        # device, so we call the default hook
        sys.__excepthook__(type, value, tb)
    else:
        import traceback
        import pdb
        # we are NOT in interactive mode, print the exception...
        traceback.print_exception(type, value, tb)
        print()
        # ...then start the debugger in post-mortem mode.
        # pdb.pm() is deprecated
        pdb.post_mortem(tb)

sys.excepthook = info

MAPPERS = {
    "skiff_all_columns": SkiffMapperSimple(),
    "classic_all_columns": ClassicMapperSimple(),

    "skiff_copy": SkiffMapperCopy(),
    "classic_copy": ClassicMapperCopy(),
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--proxy", help="proxy url")
    parser.add_argument("--src", help="source table", default=DEFAULT_SRC_TABLE)
    parser.add_argument("--nrows", help="source table", default=1 * 10**6)
    parser.add_argument("--dst", help="destination table")
    parser.add_argument("--mapper", help="type of mapper", required=True, choices=MAPPERS.keys())
    parser.add_argument("--format", help="format (only for classic mapper)")

    args = parser.parse_args()
    if args.proxy is not None:
        yt.config["proxy"]["url"] = args.proxy

    mapper = MAPPERS[args.mapper]

    if args.format is not None:
        assert not args.mapper.startswith("skiff")
        args.format = yson.loads(args.format.encode())

    if args.dst is None:
        args.dst = yt.create_temp_table()

    src_with_range = yt.TablePath(args.src, end_index=int(args.nrows))
    if args.mapper == "classic_copy":
        src_with_range.columns = COLUMNS

    yt.run_map(
        mapper,
        src_with_range,
        args.dst,
        format=args.format,
        spec={
            "auto_merge": {
                "mode": "disabled",
            },
            "data_size_per_job": 20 * 10**8,
        },
    )


if __name__ == "__main__":
    main()
