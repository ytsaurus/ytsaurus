#include "helpers.h"

#include "tablet.h"

#include <yt/yt/client/misc/io_tags.h>

#include <yt/yt/library/tracing/baggage_manager/baggage_manager.h>

namespace NYT::NTabletNode {

using namespace NTracing;
using namespace NDistributedThrottler;

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

} // namespace NYT::NTabletNode
