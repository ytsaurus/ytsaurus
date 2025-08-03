#pragma once

#include <yt/yt/library/query/base/query_common.h>

#include <yt/yt/library/query/distributed/public.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

struct TShufflePart
{
    TRowsetId RowsetId;
    std::vector<TRange<TRow>> Subranges;
};

////////////////////////////////////////////////////////////////////////////////

THashMap<TString, TShufflePart> Shuffle(
    const TShuffleNavigator& shuffleNavigator,
    TRange<TRow> rows);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
