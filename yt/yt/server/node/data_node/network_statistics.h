#pragma once

#include "public.h"

#include <library/cpp/yt/memory/ref_counted.h>

#include <yt/yt/core/profiling/public.h>

#include <yt/yt_proto/yt/client/node_tracker_client/proto/node.pb.h>

#include <yt/yt/library/syncmap/map.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

struct TNetworkCounters final
{
    std::atomic<NProfiling::TCpuInstant> ReadUpdateTime{};
    std::atomic<NProfiling::TCpuInstant> WriteUpdateTime{};
    NProfiling::TCounter ThrottledReadsCounter;
    NProfiling::TCounter ThrottledWritesCounter;
};

////////////////////////////////////////////////////////////////////////////////

class TNetworkStatistics
{
public:
    explicit TNetworkStatistics(TDataNodeConfigPtr config);

    void IncrementReadThrottlingCounter(const std::string& name);
    void IncrementWriteThrottlingCounter(const std::string& name);
    void UpdateStatistics(NNodeTrackerClient::NProto::TClusterNodeStatistics* statistics);

private:
    const TDataNodeConfigPtr Config_;

    NConcurrency::TSyncMap<std::string, TIntrusivePtr<TNetworkCounters>> Counters_;

    TNetworkCounters* GetOrCreateCounters(const std::string& name);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
