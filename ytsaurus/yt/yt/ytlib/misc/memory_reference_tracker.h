#pragma once

#include "public.h"

#include <yt/yt/core/misc/ref_counted.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT {

/////////////////////////////////////////////////////////////////////////////

struct INodeMemoryReferenceTracker
    : public TRefCounted
{
    //! Retrurs specialized tracker for specified category.
    virtual const IMemoryReferenceTrackerPtr& WithCategory(EMemoryCategory category = EMemoryCategory::Unknown) = 0;

    virtual void Reconfigure(TNodeMemoryReferenceTrackerConfigPtr config) = 0;
};

DEFINE_REFCOUNTED_TYPE(INodeMemoryReferenceTracker)

/////////////////////////////////////////////////////////////////////////////

INodeMemoryReferenceTrackerPtr CreateNodeMemoryReferenceTracker(INodeMemoryTrackerPtr tracker);

/////////////////////////////////////////////////////////////////////////////

TSharedRef TrackMemory(
    const INodeMemoryReferenceTrackerPtr& tracker,
    EMemoryCategory category,
    TSharedRef reference,
    bool keepExistingTracking = false);
TSharedRefArray TrackMemory(
    const INodeMemoryReferenceTrackerPtr& tracker,
    EMemoryCategory category,
    TSharedRefArray array,
    bool keepExistingTracking = false);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
