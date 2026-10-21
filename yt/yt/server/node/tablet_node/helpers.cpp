#include "helpers.h"

#include "store.h"
#include "tablet.h"

#include <yt/yt/client/misc/io_tags.h>

#include <yt/yt/library/tracing/baggage_manager/baggage_manager.h>

namespace NYT::NTabletNode {

using namespace NTracing;
using namespace NDistributedThrottler;
using namespace NTableClient;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

void PackBaggageFromTabletSnapshot(
    const TTraceContextPtr& context,
    ETabletIOCategory category,
    const TTabletSnapshotPtr& tabletSnapshot)
{
    if (TBaggageManager::IsBaggageAdditionEnabled()) {
        auto baggage = context->UnpackOrCreateBaggage();
        AddTagToBaggage(baggage, EAggregateIOTag::TabletCategory, FormatEnum(category));
        AddTagToBaggage(baggage, EAggregateIOTag::Bundle, tabletSnapshot->TabletCellBundle);
        context->PackBaggage(baggage);
    }
}

////////////////////////////////////////////////////////////////////////////////

EDistributedThrottlerMode GetDistributedThrottledMode(ETabletDistributedThrottlerKind kind)
{
    switch (kind) {
        case ETabletDistributedThrottlerKind::StoresUpdate:
            return EDistributedThrottlerMode::Precise;
        default:
            return EDistributedThrottlerMode::Adaptive;
    }
}

////////////////////////////////////////////////////////////////////////////////

const TColumnFilter& ColumnFilterOrUniversal(
    const TColumnFilter& columnFilter,
    const IStorePtr& store,
    TTimestamp retentionTimestamp)
{
    // ProduceSingleRowVersion uses committed writes to establish row existence.
    // Retention is applied later and may invalidate that proof: an unselected write
    // can make the row visible. Check all dynamic-store groups conservatively,
    // even when a retained write already proves existence.
    return store->IsDynamic() && retentionTimestamp != NullTimestamp
        ? TColumnFilter::MakeUniversal()
        : columnFilter;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
