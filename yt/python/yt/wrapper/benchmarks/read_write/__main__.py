import yt.yson as yson
import yt.wrapper as yt

from yt.wrapper.schema import yt_dataclass, Int64, Uint64

import argparse
from typing import Optional


DEFAULT_TABLE = "//home/dev/levysotsky/yt_bench/test1"


@yt_dataclass
class Row:
    URLClusterAlgShows: Optional[Uint64]
    VisitClickCoefGoalContextID: Optional[bytes]
    VisitCurrencyID: Optional[Uint64]
    BMCategoryHits: Optional[Uint64]
    LM7: Optional[float]
    VisitLastSignificantTraficSourceClickExpSegmentID4: Optional[bytes]
    QueryAge: Optional[Uint64]
    VisitTrafficSource_ClickCostBits: Optional[bytes]
    VisitClickDirectCampaignID: Optional[Uint64]
    AvgIncomeV2Seg1: Optional[Uint64]
    NormTypeID: Optional[Uint64]
    VisitYandexLogin: Optional[bytes]
    VisitDepth: Optional[Int64]
    ShowTime: Optional[Int64]
    HitClid: Optional[Int64]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--proxy", help="proxy url")
    parser.add_argument("--table", help="table", default=DEFAULT_TABLE)
    parser.add_argument("--nrows", help="source table", default=1 * 10**5, type=int)
    parser.add_argument("--format", help="format", required=True)

    args = parser.parse_args()
    if args.proxy is not None:
        yt.config["proxy"]["url"] = args.proxy

    table_with_range = yt.TablePath(args.table, end_index=args.nrows)

    if args.format == "structured_skiff":
        rows = yt.read_table_structured(table_with_range, Row)
    else:
        args.format = yson.loads(args.format.encode())
        rows = yt.read_table(table_with_range, format=args.format)

    c = 0
    for row in rows:
        c += 1


if __name__ == "__main__":
    main()
