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
        }

        if (subrangesForDestination.empty()) {
            continue;
        }

        shuffle[destination] = {
            .RowsetId = TGuid::Create(),
            .Subranges = std::move(subrangesForDestination),
        };
    }

    return shuffle;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
