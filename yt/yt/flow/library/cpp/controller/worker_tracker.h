#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/controller/proto/worker_tracker_service.pb.h>

#include <yt/yt/flow/library/cpp/common/worker/public.h>

#include <yt/yt/core/ytree/public.h>

#include "worker.h"

namespace NYT::NFlow::NController {

////////////////////////////////////////////////////////////////////////////////

struct IWorkerTracker
    : public TRefCounted
{
    virtual void Initialize() = 0;

    virtual std::vector<TWorkerInfo> GetWorkers() const = 0;

    virtual TWorkerInfo RegisterWorker(
        const TNodeInfoBase& nodeInfo,
        std::vector<TWorkerGroupId> groups,
        THashMap<std::string, ssize_t> capabilities) = 0;

    virtual TWorkerInfo HandleWorkerHeartbeat(
        const std::string& rpcAddress,
        NWorker::TIncarnationId connectionIncarnationId,
        ui64 heartbeatSeqNo) = 0;

    virtual void Reconfigure(const TDynamicPipelineSpecPtr& dynamicSpec) = 0;
};

DEFINE_REFCOUNTED_TYPE(IWorkerTracker);

////////////////////////////////////////////////////////////////////////////////

IWorkerTrackerPtr CreateWorkerTracker(
    TControlActionQueuePtr controlQueue,
    IYTConnectorPtr connector,
    TNodeInfoPtr nodeInfo);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NController
