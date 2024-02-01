#pragma once

#include "public.h"

#include <yt/yt/core/actions/public.h>

#include <library/cpp/yt/memory/ref_counted.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT {

/////////////////////////////////////////////////////////////////////////////

struct INodeMemoryReferenceTracker
    : public TRefCounted
{
    //! Returns specialized tracker for specified category.
    virtual const IMemoryReferenceTrackerPtr& WithCategory(
        EMemoryCategory category = EMemoryCategory::Unknown) = 0;

    virtual void Reconfigure(TNodeMemoryReferenceTrackerConfigPtr config) = 0;
};

DEFINE_REFCOUNTED_TYPE(INodeMemoryReferenceTracker)

/////////////////////////////////////////////////////////////////////////////

class TDelayedReferenceHolder
    : public TSharedRangeHolder
{
public:
    TDelayedReferenceHolder(
        TSharedRef underlying,
        TDuration delayBeforeFree,
        IInvokerPtr dtorInvoker);

    TSharedRangeHolderPtr Clone(const TSharedRangeHolderCloneOptions& options) override;

    std::optional<size_t> GetTotalByteSize() const override;

    ~TDelayedReferenceHolder() override;

private:
    TSharedRef Underlying_;
    const TDuration DelayBeforeFree_;
    const IInvokerPtr DtorInvoker_;
};

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

/////////////////////////////////////////////////////////////////////////////

IMemoryReferenceTrackerPtr WithCategory(
    const INodeMemoryReferenceTrackerPtr& tracker,
    EMemoryCategory category);

/////////////////////////////////////////////////////////////////////////////

} // namespace NYT
