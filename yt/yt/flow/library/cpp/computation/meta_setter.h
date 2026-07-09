#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/public.h>
#include <yt/yt/flow/library/cpp/common/timer.h>
#include <yt/yt/flow/library/cpp/common/visit.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TMessageParents
    : public TRefCounted
{
    std::vector<TInputMessageConstPtr> ParentMessages;
    std::vector<TInputTimerConstPtr> ParentTimers;
    std::vector<TInputVisitConstPtr> ParentVisits;

    TMessageParents(
        std::vector<TInputMessageConstPtr> parentMessages,
        std::vector<TInputTimerConstPtr> parentTimers,
        std::vector<TInputVisitConstPtr> parentVisits);
};

DEFINE_REFCOUNTED_TYPE(TMessageParents);

////////////////////////////////////////////////////////////////////////////////

struct IMetaSetter
    : public TRefCounted
{
    struct TFillResult
    {
        TMessageParentsConstPtr ActualParentMessageIds;
    };

    virtual TFillResult Fill(TMessage& message, const TMessageParentsConstPtr& parents) = 0;
    virtual TFillResult Fill(TTimer& timer, const TMessageParentsConstPtr& parents) = 0;
};

DEFINE_REFCOUNTED_TYPE(IMetaSetter);

////////////////////////////////////////////////////////////////////////////////

IMetaSetterPtr CreateUniqueMetaSetter(
    TComputationSpecPtr spec,
    const TUniqueSeqNo& uniqueSeqNo,
    TSystemTimestamp now,
    IEventTimestampAssignerPtr eventTimestampAssigner);

IMetaSetterPtr CreateDeterministicMetaSetter(
    TComputationSpecPtr spec,
    IEventTimestampAssignerPtr eventTimestampAssigner);

//! Meta setter for swift map batching (allow_batching_with_relaxed_guarantees).
//! Single-parent outputs inherit the parent's MessageId and timestamps (same as the deterministic setter).
//! Multi-parent outputs get a fresh MessageId from |uniqueSeqNo|, SystemTimestamp = max over parents,
//! EventTimestamp = min over parents. Timers are not supported.
IMetaSetterPtr CreateSwiftMergeMetaSetter(
    TComputationSpecPtr spec,
    const TUniqueSeqNo& uniqueSeqNo,
    IEventTimestampAssignerPtr eventTimestampAssigner);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
