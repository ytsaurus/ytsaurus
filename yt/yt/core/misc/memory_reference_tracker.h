#pragma once

#include "public.h"

#include <library/cpp/yt/memory/ref.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

/*!
 * Tracks memory used by references.
 *
 * Memory tracking is implemented by specific shared ref holders.
 * #Track returns reference with a combined holder: an old one and memory tracker's holder which releases memory from tracking upon destruction.
 * Subsequent track calls for returned reference drop memory reference tracker's holder unless #keepHolder=true provided.
 */
struct IMemoryReferenceTracker
    : public TRefCounted
{
     //! Tracks reference in a tracker while reference itself is alive.
    virtual TSharedRef Track(TSharedRef reference, bool keepHolder = false) = 0;
};

DEFINE_REFCOUNTED_TYPE(IMemoryReferenceTracker);

////////////////////////////////////////////////////////////////////////////////

TSharedRef TrackMemoryReference(
    const IMemoryReferenceTrackerPtr& tracker,
    TSharedRef reference,
    bool keepHolder = false);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
