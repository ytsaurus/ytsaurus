#include "shuffle.h"

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

THashMap<TString, TShufflePart> Shuffle(
    const TShuffleNavigator& shuffleNavigator,
    TRange<TRow> rows)
{
    THashMap<TString, TShufflePart> shuffle;

    for (const auto& [destination, ranges] : shuffleNavigator.DestinationMap) {
        std::vector<TRange<TRow>> subrangesForDestination;
        std::vector<TRowRange> rowsetKeyBounds;

        for (const auto& range : ranges) {
            auto begin = std::lower_bound(rows.Begin(), rows.End(), range.first, [&] (TRow lhs, TRow rhs) {
                return CompareRows(lhs, rhs, shuffleNavigator.PrefixHint) < 0;
            });
            auto end = std::lower_bound(begin, rows.End(), range.second, [&] (TRow lhs, TRow rhs) {
                return CompareValueRanges(lhs.FirstNElements(shuffleNavigator.PrefixHint), rhs.Elements()) < 0;
            });
            if (begin >= end) {
                continue;
            }

            subrangesForDestination.emplace_back(begin, end);
            rowsetKeyBounds.emplace_back(range.first, range.second);
        }

        if (subrangesForDestination.empty()) {
            continue;
        }

        TDataSource dataSource;
        dataSource.ObjectId = TGuid::Create();
        dataSource.Ranges = MakeSharedRange(std::move(rowsetKeyBounds));

        shuffle[destination] = {
            .DataSource = std::move(dataSource),
            .Subranges = std::move(subrangesForDestination),
        };
    }

    return shuffle;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
