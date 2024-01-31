#include "shuffle.h"
#include "public.h"

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

THashMap<TString, TShufflePart> Shuffle(
    const TShuffleNavigator& shuffleNavigator,
    TRange<TRow> rows,
    int prefixHint)
{
    THashMap<TString, TShufflePart> shuffle;

    for (const auto& [destination, ranges] : shuffleNavigator) {
        std::vector<TRange<TRow>> subrangesForDestination;
        std::vector<TRowRange> rowsetKeyBounds;

        for (const auto& range : ranges) {
            auto begin = std::lower_bound(rows.Begin(), rows.End(), range.first, [&] (TRow lhs, TRow rhs) {
                return CompareRows(lhs, rhs, prefixHint) < 0;
            });
            auto end = std::lower_bound(begin, rows.End(), range.second, [&] (TRow lhs, TRow rhs) {
                return CompareValueRanges(lhs.FirstNElements(prefixHint), rhs.Elements()) < 0;
            });
            if (begin >= end) {
                continue;
            }

            rowsetKeyBounds.emplace_back(*begin, *(end - 1));
            subrangesForDestination.emplace_back(begin, end);
        }

        if (subrangesForDestination.empty()) {
            continue;
        }

        auto dataSource = TDataSource{
            .ObjectId = TRowsetId::Create(),
            .Ranges = MakeSharedRange(std::move(rowsetKeyBounds)),
            .LookupSupported = true,
            .KeyWidth = static_cast<size_t>(prefixHint),
        };

        shuffle[destination] = {
            .DataSource = std::move(dataSource),
            .Subranges = std::move(subrangesForDestination),
        };
    }

    return shuffle;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
