#include "helpers.h"

#include "tablet.h"

#include <yt/yt/client/misc/io_tags.h>

#include <yt/yt/library/tracing/baggage_manager/baggage_manager.h>

namespace NYT::NTabletNode {

using namespace NTracing;

////////////////////////////////////////////////////////////////////////////////

void PackBaggageFromTabletSnapshot(const NTracing::TTraceContextPtr& context, ETabletIOCategory category, const TTabletSnapshotPtr& tabletSnapshot)
{
    if (TBaggageManager::IsBaggageAdditionEnabled()) {
        auto baggage = context->UnpackOrCreateBaggage();
        AddTagToBaggage(baggage, EAggregateIOTag::TabletCategory, FormatEnum(category));
        AddTagToBaggage(baggage, EAggregateIOTag::Bundle, tabletSnapshot->TabletCellBundle);
        context->PackBaggage(baggage);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
