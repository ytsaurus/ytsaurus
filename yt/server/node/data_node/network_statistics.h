#pragma once

#include "public.h"

#include <yt/core/misc/ref_counted.h>

#include <yt/core/concurrency/rw_spinlock.h>

#include <yt/core/profiling/profiler.h>

#include <yt/client/node_tracker_client/proto/node.pb.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

struct TNetworkCounters
{
    NProfiling::TMonotonicCounter ThrottledReadsCounter;
};

class TNetworkStatistics
{
public:
    explicit TNetworkStatistics(TDataNodeConfigPtr config);

    void IncrementReadThrottlingCounter(const TString& name);
    void UpdateStatistics(NNodeTrackerClient::NProto::TNodeStatistics* statistics);

private:
    const TDataNodeConfigPtr Config_;

    NConcurrency::TReaderWriterSpinLock Lock_;
    THashMap<TString, TNetworkCounters> Counters_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
