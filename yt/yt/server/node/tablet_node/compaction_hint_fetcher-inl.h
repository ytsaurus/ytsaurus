#ifndef COMPACTION_HINT_FETCHER_INL_H_
#error "Direct inclusion of this file is not allowed, include compaction_hint_fetcher.h"
// For the sake of sane code completion.
#include "compaction_hint_fetcher.h"
#endif

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

template <class TResult>
void Serialize(
    const TCompactionHint<TResult>& compactionHint,
    NYson::IYsonConsumer* consumer)
{
    NYTree::BuildYsonFluently(consumer).BeginMap()
        .DoIf(compactionHint.CompactionHint.has_value(), [&] (auto fluent) {
            fluent.Item("compaction_hint").Value(*compactionHint.CompactionHint);
        })
        .DoIf(!compactionHint.CompactionHint.has_value(), [&] (auto fluent) {
            fluent.Item("fetch_status").Value(compactionHint.FetchStatus);
        })
    .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
