#pragma once

#include "public.h"

#include <yt/yt/server/node/data_node/public.h>

#include <yt/yt/server/node/job_agent/public.h>

#include <yt/yt/server/lib/job_agent/public.h>

#include <yt/yt/server/lib/job_proxy/public.h>

#include <yt/yt/server/lib/nbd/server.h>

#include <yt/yt/server/node/cluster_node/bootstrap.h>

#include <yt/yt/ytlib/scheduler/cluster_name.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

struct IBootstrap
    : public virtual NClusterNode::IBootstrapBase
{
    virtual ~IBootstrap() = default;

    virtual void Initialize() = 0;
    virtual void Run() = 0;

    virtual const IJobInputCachePtr& GetJobInputCache() const = 0;

    virtual const TGpuManagerPtr& GetGpuManager() const = 0;

    virtual const TSlotManagerPtr& GetSlotManager() const = 0;

    virtual const NServer::TJobReporterPtr& GetJobReporter() const = 0;

    virtual const NJobProxy::TJobProxyInternalConfigPtr& GetJobProxyConfigTemplate() const = 0;

    virtual const TChunkCachePtr& GetChunkCache() const = 0;

    virtual bool IsSimpleEnvironment() const = 0;

    virtual const IJobControllerPtr& GetJobController() const = 0;

    virtual const IMasterConnectorPtr& GetMasterConnector() const = 0;

    virtual const TSchedulerConnectorPtr& GetSchedulerConnector() const = 0;

    virtual NConcurrency::IThroughputThrottlerPtr GetThrottler(
        EExecNodeThrottlerKind kind,
        EThrottlerTrafficType trafficType = EThrottlerTrafficType::Bandwidth,
        std::optional<NScheduler::TClusterName> remoteClusterName = std::nullopt) const = 0;

    virtual const NProfiling::TSolomonExporterPtr& GetJobProxySolomonExporter() const = 0;

    virtual const TControllerAgentConnectorPoolPtr& GetControllerAgentConnectorPool() const = 0;

    virtual NClusterNode::TClusterNodeDynamicConfigPtr GetDynamicConfig() const = 0;

    virtual NYT::NNbd::INbdServerPtr GetNbdServer() const = 0;

    virtual NChunkClient::TChunkReaderHostPtr GetLayerReaderHost() const = 0;

    virtual NChunkClient::TChunkReaderHostPtr GetFileReaderHost() const = 0;

    virtual const IJobProxyLogManagerPtr& GetJobProxyLogManager() const = 0;

    virtual IThrottlerManagerPtr GetThrottlerManager() const = 0;

    virtual void UpdateNodeProfilingTags(std::vector<NProfiling::TTag> dynamicTags) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IBootstrap)

////////////////////////////////////////////////////////////////////////////////

IBootstrapPtr CreateBootstrap(NClusterNode::IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
