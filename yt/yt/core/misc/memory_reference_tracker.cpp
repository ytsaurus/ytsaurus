#include "memory_reference_tracker.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TSharedRef TrackMemoryReference(
    const IMemoryReferenceTrackerPtr& tracker,
    TSharedRef reference,
    bool keepHolder)
{
    return tracker
        ? tracker->Track(reference, keepHolder)
        : reference;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
