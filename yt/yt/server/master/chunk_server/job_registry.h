#pragma once

#include "private.h"
#include "chunk.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/node_tracker_server/data_center.h>

#include <yt/yt/server/lib/misc/max_min_balancer.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/client/job_tracker_client/helpers.h>

#include <yt/yt/library/erasure/public.h>

#include <yt/yt/library/profiling/producer.h>

#include <yt/yt/core/concurrency/public.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/property.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/ytlib/node_tracker_client/proto/node_tracker_service.pb.h>

#include <functional>
#include <deque>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

struct IJobRegistry
    : public TRefCounted
{
    virtual void RegisterJob(TJobPtr job) = 0;
    virtual void OnJobFinished(TJobPtr job) = 0;

    virtual TJobPtr FindJob(TJobId jobId) = 0;

    virtual TJobPtr FindLastFinishedJob(TChunkId chunkId) const = 0;

    virtual const THashSet<TJobPtr>& GetNodeJobs(const TString& nodeAddress) const = 0;

    virtual int GetJobCount(EJobType jobType) const = 0;

    virtual TJobEpoch StartEpoch() = 0;
    virtual void OnEpochFinished(TJobEpoch epoch) = 0;

    virtual bool IsOverdraft() const = 0;
    virtual bool IsOverdraft(EJobType jobType) const = 0;

    virtual void OverrideResourceLimits(
        NNodeTrackerClient::NProto::TNodeResources* resourceLimits,
        const TNode& node) = 0;

    virtual void OnProfiling(NProfiling::TSensorBuffer* buffer) = 0;
};

DEFINE_REFCOUNTED_TYPE(IJobRegistry)

////////////////////////////////////////////////////////////////////////////////

IJobRegistryPtr CreateJobRegistry(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
