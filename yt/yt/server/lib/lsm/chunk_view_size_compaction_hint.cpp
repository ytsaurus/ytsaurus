#include "tablet.h"

#include <yt/yt/server/lib/tablet_node/config.h>

namespace NYT::NLsm {

////////////////////////////////////////////////////////////////////////////////

template <>
void DoRecalculateStoreCompactionHint<EStoreCompactionHintKind::ChunkViewTooNarrow>(TStore* store)
{
    auto& hint = store->CompactionHints().Hints()[EStoreCompactionHintKind::ChunkViewTooNarrow];
    auto chunkViewShare = std::get<TStoreCompactionHint::TChunkViewTooNarrowPayload>(
        store->CompactionHints().Payloads()[EStoreCompactionHintKind::ChunkViewTooNarrow]);

    hint.MakeDecision(
        TInstant::Zero(),
        chunkViewShare <= store->GetTablet()->GetMountConfig()->MaxChunkViewSizeRatio
            ? EStoreCompactionReason::NarrowChunkView
            : EStoreCompactionReason::None);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm
