#include "tablet.h"

#include <yt/yt/server/lib/tablet_node/config.h>

namespace NYT::NLsm {

////////////////////////////////////////////////////////////////////////////////

template <>
void DoRecalculateStoreCompactionHint<EStoreCompactionHintKind::ChunkViewTooNarrow>(TStore* store)
{
    auto recalculationFinalizer = store->CompactionHints().Hints()[EStoreCompactionHintKind::ChunkViewTooNarrow]
        .BuildRecalculationFinalizer(store);

    auto chunkViewShare = std::get<TStoreCompactionHint::TChunkViewTooNarrowPayload>(
        store->CompactionHints().Payloads()[EStoreCompactionHintKind::ChunkViewTooNarrow]);

    if (chunkViewShare <= store->GetTablet()->GetMountConfig()->MaxChunkViewSizeRatio) {
        recalculationFinalizer.TryApplyRecalculation(TInstant::Zero(), EStoreCompactionReason::NarrowChunkView);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm
