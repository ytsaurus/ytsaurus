#pragma once

#include "private.h"
#include "chunk.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/node_tracker_server/data_center.h>

#include <yt/yt/server/lib/misc/max_min_balancer.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/library/erasure/public.h>

#include <yt/yt/library/profiling/producer.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/optional.h>
#include <yt/yt/core/misc/property.h>
#include <yt/yt/core/misc/small_set.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/ytlib/node_tracker_client/proto/node_tracker_service.pb.h>

#include <functional>
#include <deque>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TJobRegistry
    : public TRefCounted
{
public:
    TJobRegistry(
        TChunkManagerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap);
    ~TJobRegistry();

    void RegisterJob(const TJobPtr& job);
    void UnregisterJob(TJobPtr job);

    void Start();
    void Stop();

    bool IsOverdraft() const;

    void OverrideResourceLimits(NNodeTrackerClient::NProto::TNodeResources* resourceLimits, const TNode& node);

    void OnProfiling(NProfiling::TSensorBuffer* buffer) const;

private:
    const TChunkManagerConfigPtr Config_;
    NCellMaster::TBootstrap* const Bootstrap_;

    using TJobCounters = TEnumIndexedVector<EJobType, i64, NJobTrackerClient::FirstMasterJobType, NJobTrackerClient::LastMasterJobType>;
    // Number of jobs running - per job type. For profiling.
    TJobCounters RunningJobs_;

    TJobCounters JobsStarted_;
    TJobCounters JobsCompleted_;
    TJobCounters JobsFailed_;
    TJobCounters JobsAborted_;

    const NConcurrency::IReconfigurableThroughputThrottlerPtr JobThrottler_;

    const TCallback<void(NCellMaster::TDynamicClusterConfigPtr)> DynamicConfigChangedCallback_ =
        BIND(&TJobRegistry::OnDynamicConfigChanged, MakeWeak(this));

    const TDynamicChunkManagerConfigPtr& GetDynamicConfig();
    void OnDynamicConfigChanged(NCellMaster::TDynamicClusterConfigPtr /*oldConfig*/ = nullptr);
};

DEFINE_REFCOUNTED_TYPE(TJobRegistry)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
