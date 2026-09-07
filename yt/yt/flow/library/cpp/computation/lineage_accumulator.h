#pragma once

#include "meta_setter.h"

#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/timer.h>
#include <yt/yt/flow/library/cpp/common/traverse.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void AddLineageInput(
    TLineageDelta* delta,
    const TComputationSpecPtr& spec,
    const TStreamId& inputStreamId,
    i64 count,
    i64 byteSize);

void AddLineageInputs(
    TLineageDelta* delta,
    const TComputationSpecPtr& spec,
    const std::vector<TInputMessageConstPtr>& messages,
    const std::vector<TInputTimerConstPtr>& timers,
    const std::vector<TInputVisitConstPtr>& visits);

class TLineageAccumulator
{
public:
    void Add(const TMessage& output, const TMessageParentsConstPtr& parents);
    void Add(const TTimer& output, const TMessageParentsConstPtr& parents);

    TLineageDelta Finish();

private:
    TLineageDelta SingleParentDelta_;

    struct TOutputGroup
    {
        TMessageParentsConstPtr Parents;
        THashMap<TStreamId, TLineageDeltaValue> OutputDeltas;
    };

    THashMap<const TMessageParents*, TOutputGroup> MultiParentGroups_;

    void DoAdd(
        const TStreamId& outputStreamId,
        i64 outputByteSize,
        const TMessageParentsConstPtr& parents);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
